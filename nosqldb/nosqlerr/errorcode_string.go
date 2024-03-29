// Code generated by "stringer -type=ErrorCode -output errorcode_string.go"; DO NOT EDIT.

package nosqlerr

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[NoError-0]
	_ = x[UnknownOperation-1]
	_ = x[TableNotFound-2]
	_ = x[IndexNotFound-3]
	_ = x[IllegalArgument-4]
	_ = x[RowSizeLimitExceeded-5]
	_ = x[KeySizeLimitExceeded-6]
	_ = x[BatchOpNumberLimitExceeded-7]
	_ = x[RequestSizeLimitExceeded-8]
	_ = x[TableExists-9]
	_ = x[IndexExists-10]
	_ = x[InvalidAuthorization-11]
	_ = x[InsufficientPermission-12]
	_ = x[ResourceExists-13]
	_ = x[ResourceNotFound-14]
	_ = x[TableLimitExceeded-15]
	_ = x[IndexLimitExceeded-16]
	_ = x[BadProtocolMessage-17]
	_ = x[EvolutionLimitExceeded-18]
	_ = x[TableDeploymentLimitExceeded-19]
	_ = x[TenantDeploymentLimitExceeded-20]
	_ = x[OperationNotSupported-21]
	_ = x[EtagMismatch-22]
	_ = x[CannotCancelWorkRequest-23]
	_ = x[UnsupportedProtocol-24]
	_ = x[Error25-25]
	_ = x[TableNotReady-26]
	_ = x[UnsupportedQueryVersion-27]
	_ = x[ReadLimitExceeded-50]
	_ = x[WriteLimitExceeded-51]
	_ = x[SizeLimitExceeded-52]
	_ = x[OperationLimitExceeded-53]
	_ = x[RequestTimeout-100]
	_ = x[ServerError-101]
	_ = x[ServiceUnavailable-102]
	_ = x[TableBusy-103]
	_ = x[SecurityInfoUnavailable-104]
	_ = x[RetryAuthentication-105]
	_ = x[UnknownError-125]
	_ = x[IllegalState-126]
}

const (
	_ErrorCode_name_0 = "NoErrorUnknownOperationTableNotFoundIndexNotFoundIllegalArgumentRowSizeLimitExceededKeySizeLimitExceededBatchOpNumberLimitExceededRequestSizeLimitExceededTableExistsIndexExistsInvalidAuthorizationInsufficientPermissionResourceExistsResourceNotFoundTableLimitExceededIndexLimitExceededBadProtocolMessageEvolutionLimitExceededTableDeploymentLimitExceededTenantDeploymentLimitExceededOperationNotSupportedEtagMismatchCannotCancelWorkRequestUnsupportedProtocolError25TableNotReadyUnsupportedQueryVersion"
	_ErrorCode_name_1 = "ReadLimitExceededWriteLimitExceededSizeLimitExceededOperationLimitExceeded"
	_ErrorCode_name_2 = "RequestTimeoutServerErrorServiceUnavailableTableBusySecurityInfoUnavailableRetryAuthentication"
	_ErrorCode_name_3 = "UnknownErrorIllegalState"
)

var (
	_ErrorCode_index_0 = [...]uint16{0, 7, 23, 36, 49, 64, 84, 104, 130, 154, 165, 176, 196, 218, 232, 248, 266, 284, 302, 324, 352, 381, 402, 414, 437, 456, 463, 476, 499}
	_ErrorCode_index_1 = [...]uint8{0, 17, 35, 52, 74}
	_ErrorCode_index_2 = [...]uint8{0, 14, 25, 43, 52, 75, 94}
	_ErrorCode_index_3 = [...]uint8{0, 12, 24}
)

func (i ErrorCode) String() string {
	switch {
	case 0 <= i && i <= 27:
		return _ErrorCode_name_0[_ErrorCode_index_0[i]:_ErrorCode_index_0[i+1]]
	case 50 <= i && i <= 53:
		i -= 50
		return _ErrorCode_name_1[_ErrorCode_index_1[i]:_ErrorCode_index_1[i+1]]
	case 100 <= i && i <= 105:
		i -= 100
		return _ErrorCode_name_2[_ErrorCode_index_2[i]:_ErrorCode_index_2[i+1]]
	case 125 <= i && i <= 126:
		i -= 125
		return _ErrorCode_name_3[_ErrorCode_index_3[i]:_ErrorCode_index_3[i+1]]
	default:
		return "ErrorCode(" + strconv.FormatInt(int64(i), 10) + ")"
	}
}
