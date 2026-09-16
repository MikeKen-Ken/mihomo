package outboundgroup

// PrepareManualSelectionReset captures the selection owned by the start of a
// delay test. The returned completion only clears that generation, preserving
// a user choice made while the test was running (even the same node again).
func (f *Fallback) PrepareManualSelectionReset() func() {
	selection := f.selection.snapshot()
	return func() { f.clearManualSelectionIfUnchanged(selection) }
}

func (u *URLTest) PrepareManualSelectionReset() func() {
	selection := u.selection.snapshot()
	return func() { u.clearManualSelectionIfUnchanged(selection) }
}
