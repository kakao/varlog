package mrpb

func (rec *StateMachineLogRecord) Validate(crc uint32) bool {
	if rec.Crc == crc {
		return true
	}
	return false
}
