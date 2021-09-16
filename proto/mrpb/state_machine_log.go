package mrpb

func (rec *StateMachineLogRecord) Validate(crc uint32) bool {
	return rec.Crc == crc
}
