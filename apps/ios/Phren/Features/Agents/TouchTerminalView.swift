import SwiftTerm
import UIKit

/// Phone gestures are deliberately different from desktop pointer gestures.
/// Keep taps for TUI controls, scroll with a swipe, and select only after a hold.
final class TouchTerminalView: TerminalView, UIGestureRecognizerDelegate, UIEditMenuInteractionDelegate {
    private var wheelPan: UIPanGestureRecognizer?
    private var tuiTap: UITapGestureRecognizer?
    private var shellTaps: [UITapGestureRecognizer] = []
    private var wheelRemainder: CGFloat = 0
    private lazy var editMenu = UIEditMenuInteraction(delegate: self)
    #if DEBUG && targetEnvironment(simulator)
    private(set) var copyActions = 0
    #endif

    func configureTouchInput() {
        // SwiftUI owns the one keyboard toolbar. SwiftTerm installs another by default.
        inputAccessoryView = nil
        shellTaps = (gestureRecognizers ?? []).compactMap { $0 as? UITapGestureRecognizer }
        for gesture in gestureRecognizers ?? [] where gesture is UILongPressGestureRecognizer {
            removeGestureRecognizer(gesture)
        }
        let hold = UILongPressGestureRecognizer(target: self, action: #selector(selectText(_:)))
        hold.minimumPressDuration = 0.45
        addGestureRecognizer(hold)
        let wheel = UIPanGestureRecognizer(target: self, action: #selector(scrollTerminal(_:)))
        wheel.maximumNumberOfTouches = 1
        wheel.delegate = self
        wheel.require(toFail: hold)
        addGestureRecognizer(wheel)
        wheelPan = wheel
        let tap = UITapGestureRecognizer(target: self, action: #selector(tapTerminal(_:)))
        tap.require(toFail: hold)
        tap.require(toFail: wheel)
        addGestureRecognizer(tap)
        tuiTap = tap
        addInteraction(editMenu)
        updateScrollGestures()
        accessibilityHint = "Swipe to scroll. Touch and hold to select text, then choose Copy or Paste."
    }

    override func mouseModeChanged(source: Terminal) {
        // Do not install SwiftTerm's desktop mouse-drag recognizer. Herdr interprets
        // those presses/motions as remote selection, including copy on release.
        updateScrollGestures()
    }

    override func selectionChanged(source: Terminal) {
        super.selectionChanged(source: source)
        updateScrollGestures()
    }

    private func updateScrollGestures() {
        guard wheelPan != nil else { return }
        // A tap dismissing local selection must not click a remote TUI control.
        allowMouseReporting = !hasActiveSelection
        panGestureRecognizer.isEnabled = getTerminal().mouseMode == .off && !hasActiveSelection
        wheelPan?.isEnabled = getTerminal().mouseMode != .off || hasActiveSelection
        let localShell = getTerminal().mouseMode == .off && !hasActiveSelection
        shellTaps.forEach { $0.isEnabled = localShell }
        tuiTap?.isEnabled = !localShell
    }

    override func paste(_ sender: Any?) {
        super.paste(sender)
        clearSelection()
    }

    override func copy(_ sender: Any?) {
        #if DEBUG && targetEnvironment(simulator)
        copyActions += 1
        #endif
        super.copy(sender)
    }

    func editMenuInteraction(_ interaction: UIEditMenuInteraction, menuFor configuration: UIEditMenuConfiguration,
                             suggestedActions: [UIMenuElement]) -> UIMenu? {
        UIMenu(children: [
            UIAction(title: "Copy", image: UIImage(systemName: "doc.on.doc"),
                     attributes: hasActiveSelection ? [] : .disabled) { [weak self] _ in self?.copy(nil) },
            UIAction(title: "Paste", image: UIImage(systemName: "document.on.clipboard")) { [weak self] _ in self?.paste(nil) },
            UIAction(title: "Select All", attributes: .keepsMenuPresented) { [weak self] _ in
                self?.selection.selectAll()
            }
        ])
    }

    override func gestureRecognizerShouldBegin(_ gestureRecognizer: UIGestureRecognizer) -> Bool {
        guard gestureRecognizer === wheelPan, let pan = gestureRecognizer as? UIPanGestureRecognizer else { return true }
        if hasActiveSelection { return true }
        let velocity = pan.velocity(in: self)
        return !hasActiveSelection && abs(velocity.y) > abs(velocity.x)
    }

    private var cellSize: CGSize {
        // Use the terminal's point geometry, not the screen's Retina scale.
        // A UIWindow's contentScaleFactor can differ from UIScreen.scale.
        let core = getTerminal(), frame = getOptimalFrameSize()
        return CGSize(width: max(1, frame.width / CGFloat(max(1, core.cols))),
                      height: max(1, frame.height / CGFloat(max(1, core.rows))))
    }

    @objc private func tapTerminal(_ gesture: UITapGestureRecognizer) {
        guard gesture.state == .ended else { return }
        if hasActiveSelection {
            clearSelection()
            editMenu.dismissMenu()
            return
        }
        let core = getTerminal()
        guard core.mouseMode != .off else { return }
        let point = gesture.location(in: self)
        if let link = core.link(at: .buffer(bufferPosition(at: point)), mode: .explicitAndImplicit) {
            terminalDelegate?.requestOpenLink(source: self, link: link, params: [:])
            return
        }
        let viewport = CGPoint(x: point.x - bounds.minX, y: point.y - bounds.minY)
        let column = max(0, min(core.cols - 1, Int(viewport.x / cellSize.width)))
        let row = max(0, min(core.rows - 1, Int(viewport.y / cellSize.height)))
        for release in [false, true] {
            if release && core.mouseMode == .x10 { continue }
            let flags = core.encodeButton(button: 0, release: release, shift: false, meta: false, control: false)
            core.sendEvent(buttonFlags: flags, x: column, y: row,
                           pixelX: max(0, Int(viewport.x)), pixelY: max(0, Int(viewport.y)))
        }
        becomeFirstResponder()
    }

    @objc private func scrollTerminal(_ gesture: UIPanGestureRecognizer) {
        if hasActiveSelection {
            let point = gesture.location(in: self)
            let position = bufferPosition(at: point)
            switch gesture.state {
            case .began:
                editMenu.dismissMenu()
                let start = selection.start, end = selection.end
                let offset = position.row * getTerminal().cols + position.col
                let startOffset = start.row * getTerminal().cols + start.col
                let endOffset = end.row * getTerminal().cols + end.col
                selection.pivot = abs(offset - startOffset) < abs(offset - endOffset) ? end : start
            case .changed: selection.pivotExtend(bufferPosition: position)
            case .ended: editMenu.presentEditMenu(with: UIEditMenuConfiguration(identifier: nil, sourcePoint: point))
            default: break
            }
            return
        }
        guard getTerminal().mouseMode != .off else { return }
        if gesture.state == .began { wheelRemainder = 0 }
        let delta = gesture.translation(in: self).y
        gesture.setTranslation(.zero, in: self)
        wheelRemainder += delta
        let lines = Int(wheelRemainder / cellSize.height)
        guard lines != 0 else { return }
        wheelRemainder -= CGFloat(lines) * cellSize.height
        let core = getTerminal()
        let point = gesture.location(in: self)
        let viewport = CGPoint(x: point.x - bounds.minX, y: point.y - bounds.minY)
        let column = max(0, min(core.cols - 1, Int(viewport.x / cellSize.width)))
        let row = max(0, min(core.rows - 1, Int(viewport.y / cellSize.height)))
        let flags = core.encodeButton(button: lines > 0 ? 4 : 5, release: false,
                                      shift: false, meta: false, control: false)
        for _ in 0..<abs(lines) {
            core.sendEvent(buttonFlags: flags, x: column, y: row,
                           pixelX: max(0, Int(viewport.x)), pixelY: max(0, Int(viewport.y)))
        }
    }

    @objc private func selectText(_ gesture: UILongPressGestureRecognizer) {
        let point = gesture.location(in: self)
        let core = getTerminal()
        let position = bufferPosition(at: point)
        switch gesture.state {
        case .began:
            editMenu.dismissMenu()
            selection.selectWordOrExpression(at: position, in: core.buffer)
            selection.selectionMode = .word
            UISelectionFeedbackGenerator().selectionChanged()
        case .changed:
            selection.dragExtend(bufferPosition: position)
        case .ended:
            editMenu.presentEditMenu(with: UIEditMenuConfiguration(identifier: nil, sourcePoint: point))
        case .cancelled:
            clearSelection()
        default: break
        }
    }

    private func bufferPosition(at point: CGPoint) -> Position {
        let core = getTerminal()
        return Position(col: max(0, min(core.cols - 1, Int(point.x / cellSize.width))),
                        row: max(core.getTopVisibleRow(), min(core.getTopVisibleRow() + core.rows - 1,
                                                            Int(point.y / cellSize.height))))
    }
}
