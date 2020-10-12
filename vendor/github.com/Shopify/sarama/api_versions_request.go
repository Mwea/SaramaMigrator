package sarama

//ApiVersionsRequest ...
type ApiVersionsRequest struct {
	Version               int16
	ClientSoftwareName    string
	ClientSoftwareVersion string
}

func (a *ApiVersionsRequest) encode(pe packetEncoder) error {
	pe.putString(a.ClientSoftwareName)
	pe.putString(a.ClientSoftwareVersion)
	pe.putInt16(a.Version)
	return nil
}

func (a *ApiVersionsRequest) encode(pe packetEncoder) error {
	return nil
}

func (a *ApiVersionsRequest) decode(pd packetDecoder, version int16) (err error) {
	if version >= 3 {
		if s, err := pd.getCompactString(); err != nil {
			return err
		} else {
			a.ClientSoftwareName = s
		}
		if s, err := pd.getCompactString(); err != nil {
			return nil
		} else {
			a.ClientSoftwareVersion = string(s)
		}
	}
	return nil
}

func (a *ApiVersionsRequest) key() int16 {
	return 18
}

func (a *ApiVersionsRequest) version() int16 {
	return 0
}

func (a *ApiVersionsRequest) headerVersion() int16 {
	return 1
}

func (a *ApiVersionsRequest) requiredVersion() KafkaVersion {
	return V0_10_0_0
}
