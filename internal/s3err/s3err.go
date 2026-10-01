// Package s3err defines the S3 error codes strata returns, with the HTTP
// status S3 uses for each.
package s3err

import (
	"errors"
	"fmt"
	"net/http"
)

// Error is an S3 error: a code clients switch on, a message for humans, and
// the HTTP status.
type Error struct {
	Code    string
	Message string
	Status  int
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Is lets errors.Is match on the code, so a customised message still matches
// the predefined value.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// With returns a copy of e with a different message.
func (e *Error) With(format string, args ...any) *Error {
	c := *e
	c.Message = fmt.Sprintf(format, args...)
	return &c
}

func def(code string, status int, msg string) *Error {
	return &Error{Code: code, Message: msg, Status: status}
}

// The error codes, as documented in the S3 API reference.
var (
	AccessDenied                 = def("AccessDenied", http.StatusForbidden, "Access Denied")
	AuthorizationHeaderMalformed = def("AuthorizationHeaderMalformed", http.StatusBadRequest, "The authorization header is malformed.")
	AuthorizationQueryMalformed  = def("AuthorizationQueryParametersError", http.StatusBadRequest, "Error parsing the X-Amz-Credential parameter.")
	BadDigest                    = def("BadDigest", http.StatusBadRequest, "The Content-MD5 you specified did not match what we received.")
	BucketAlreadyExists          = def("BucketAlreadyExists", http.StatusConflict, "The requested bucket name is not available.")
	BucketAlreadyOwnedByYou      = def("BucketAlreadyOwnedByYou", http.StatusConflict, "Your previous request to create the named bucket succeeded and you already own it.")
	BucketNotEmpty               = def("BucketNotEmpty", http.StatusConflict, "The bucket you tried to delete is not empty.")
	EntityTooLarge               = def("EntityTooLarge", http.StatusBadRequest, "Your proposed upload exceeds the maximum allowed object size.")
	EntityTooSmall               = def("EntityTooSmall", http.StatusBadRequest, "Your proposed upload is smaller than the minimum allowed object size.")
	IncompleteBody               = def("IncompleteBody", http.StatusBadRequest, "You did not provide the number of bytes specified by the Content-Length HTTP header.")
	InternalError                = def("InternalError", http.StatusInternalServerError, "We encountered an internal error. Please try again.")
	InvalidAccessKeyID           = def("InvalidAccessKeyId", http.StatusForbidden, "The AWS access key ID you provided does not exist in our records.")
	InvalidArgument              = def("InvalidArgument", http.StatusBadRequest, "Invalid Argument")
	InvalidBucketName            = def("InvalidBucketName", http.StatusBadRequest, "The specified bucket is not valid.")
	InvalidDigest                = def("InvalidDigest", http.StatusBadRequest, "The Content-MD5 you specified is not valid.")
	InvalidPart                  = def("InvalidPart", http.StatusBadRequest, "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.")
	InvalidPartOrder             = def("InvalidPartOrder", http.StatusBadRequest, "The list of parts was not in ascending order. The parts list must be specified in order by part number.")
	InvalidRange                 = def("InvalidRange", http.StatusRequestedRangeNotSatisfiable, "The requested range is not satisfiable")
	InvalidRequest               = def("InvalidRequest", http.StatusBadRequest, "Invalid Request")
	InvalidStorageClass          = def("InvalidStorageClass", http.StatusBadRequest, "The storage class you specified is not valid.")
	KeyTooLong                   = def("KeyTooLongError", http.StatusBadRequest, "Your key is too long.")
	MalformedXML                 = def("MalformedXML", http.StatusBadRequest, "The XML you provided was not well-formed or did not validate against our published schema.")
	MetadataTooLarge             = def("MetadataTooLarge", http.StatusBadRequest, "Your metadata headers exceed the maximum allowed metadata size.")
	MethodNotAllowed             = def("MethodNotAllowed", http.StatusMethodNotAllowed, "The specified method is not allowed against this resource.")
	MissingContentLength         = def("MissingContentLength", http.StatusLengthRequired, "You must provide the Content-Length HTTP header.")
	MissingRequestBody           = def("MissingRequestBodyError", http.StatusBadRequest, "Request Body is empty.")
	MissingSecurityHeader        = def("MissingSecurityHeader", http.StatusBadRequest, "Your request is missing a required header.")
	NoSuchBucket                 = def("NoSuchBucket", http.StatusNotFound, "The specified bucket does not exist.")
	NoSuchKey                    = def("NoSuchKey", http.StatusNotFound, "The specified key does not exist.")
	NoSuchUpload                 = def("NoSuchUpload", http.StatusNotFound, "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")
	NotImplemented               = def("NotImplemented", http.StatusNotImplemented, "A header you provided implies functionality that is not implemented.")
	NotModified                  = def("NotModified", http.StatusNotModified, "Not Modified")
	PreconditionFailed           = def("PreconditionFailed", http.StatusPreconditionFailed, "At least one of the pre-conditions you specified did not hold")
	ConditionalRequestConflict   = def("ConditionalRequestConflict", http.StatusConflict, "A conflicting conditional operation is currently in progress against this resource.")
	RequestTimeTooSkewed         = def("RequestTimeTooSkewed", http.StatusForbidden, "The difference between the request time and the server's time is too large.")
	SignatureDoesNotMatch        = def("SignatureDoesNotMatch", http.StatusForbidden, "The request signature we calculated does not match the signature you provided. Check your key and signing method.")
	SlowDown                     = def("SlowDown", http.StatusServiceUnavailable, "Please reduce your request rate.")
	XAmzContentSHA256Mismatch    = def("XAmzContentSHA256Mismatch", http.StatusBadRequest, "The provided 'x-amz-content-sha256' header does not match what was computed.")
	InvalidChunkSize             = def("InvalidChunkSizeError", http.StatusBadRequest, "Only the last chunk is allowed to have a size less than 8192 bytes")
	MalformedPOSTRequest         = def("IncompleteBody", http.StatusBadRequest, "The body of the aws-chunked request is malformed.")
	TooManyParts                 = def("InvalidArgument", http.StatusBadRequest, "Part number must be an integer between 1 and 10000, inclusive")
)

// From converts any error into an S3 error, mapping unknown errors to
// InternalError.
func From(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return InternalError
}
