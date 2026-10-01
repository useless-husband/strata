package s3api

import (
	"encoding/xml"
	"io"
	"net/http"
	"time"

	"github.com/useless-husband/strata/internal/s3err"
)

const s3NS = "http://s3.amazonaws.com/doc/2006-03-01/"

// iso8601 is the timestamp format of S3 XML bodies.
func iso8601(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type errorResponse struct {
	XMLName    xml.Name `xml:"Error"`
	Code       string   `xml:"Code"`
	Message    string   `xml:"Message"`
	Key        string   `xml:"Key,omitempty"`
	BucketName string   `xml:"BucketName,omitempty"`
	Resource   string   `xml:"Resource"`
	RequestID  string   `xml:"RequestId"`
	HostID     string   `xml:"HostId"`
}

type listAllMyBucketsResult struct {
	XMLName           xml.Name    `xml:"ListAllMyBucketsResult"`
	NS                string      `xml:"xmlns,attr"`
	Owner             owner       `xml:"Owner"`
	Buckets           []bucketXML `xml:"Buckets>Bucket"`
	ContinuationToken string      `xml:"ContinuationToken,omitempty"`
	Prefix            string      `xml:"Prefix,omitempty"`
}

type bucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
	BucketRegion string `xml:"BucketRegion,omitempty"`
}

type objectXML struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	Owner        *owner `xml:"Owner,omitempty"`
}

type commonPrefixXML struct {
	Prefix string `xml:"Prefix"`
}

type listBucketResultV2 struct {
	XMLName               xml.Name          `xml:"ListBucketResult"`
	NS                    string            `xml:"xmlns,attr"`
	Name                  string            `xml:"Name"`
	Prefix                string            `xml:"Prefix"`
	ContinuationToken     *string           `xml:"ContinuationToken"`
	NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
	StartAfter            string            `xml:"StartAfter,omitempty"`
	KeyCount              int               `xml:"KeyCount"`
	MaxKeys               int               `xml:"MaxKeys"`
	Delimiter             string            `xml:"Delimiter,omitempty"`
	EncodingType          string            `xml:"EncodingType,omitempty"`
	IsTruncated           bool              `xml:"IsTruncated"`
	Contents              []objectXML       `xml:"Contents"`
	CommonPrefixes        []commonPrefixXML `xml:"CommonPrefixes"`
}

type listBucketResultV1 struct {
	XMLName        xml.Name          `xml:"ListBucketResult"`
	NS             string            `xml:"xmlns,attr"`
	Name           string            `xml:"Name"`
	Prefix         string            `xml:"Prefix"`
	Marker         string            `xml:"Marker"`
	NextMarker     string            `xml:"NextMarker,omitempty"`
	MaxKeys        int               `xml:"MaxKeys"`
	Delimiter      string            `xml:"Delimiter,omitempty"`
	EncodingType   string            `xml:"EncodingType,omitempty"`
	IsTruncated    bool              `xml:"IsTruncated"`
	Contents       []objectXML       `xml:"Contents"`
	CommonPrefixes []commonPrefixXML `xml:"CommonPrefixes"`
}

type versionXML struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	Owner        owner  `xml:"Owner"`
}

type listVersionsResult struct {
	XMLName             xml.Name          `xml:"ListVersionsResult"`
	NS                  string            `xml:"xmlns,attr"`
	Name                string            `xml:"Name"`
	Prefix              string            `xml:"Prefix"`
	KeyMarker           string            `xml:"KeyMarker"`
	VersionIDMarker     string            `xml:"VersionIdMarker"`
	NextKeyMarker       string            `xml:"NextKeyMarker,omitempty"`
	NextVersionIDMarker string            `xml:"NextVersionIdMarker,omitempty"`
	MaxKeys             int               `xml:"MaxKeys"`
	Delimiter           string            `xml:"Delimiter,omitempty"`
	EncodingType        string            `xml:"EncodingType,omitempty"`
	IsTruncated         bool              `xml:"IsTruncated"`
	Versions            []versionXML      `xml:"Version"`
	CommonPrefixes      []commonPrefixXML `xml:"CommonPrefixes"`
}

type locationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	NS      string   `xml:"xmlns,attr"`
	Value   string   `xml:",chardata"`
}

type createBucketConfiguration struct {
	XMLName            xml.Name `xml:"CreateBucketConfiguration"`
	LocationConstraint string   `xml:"LocationConstraint"`
}

type versioningConfiguration struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	NS      string   `xml:"xmlns,attr"`
	Status  string   `xml:"Status,omitempty"`
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	NS           string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

type copyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	NS           string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

type deleteRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key       string `xml:"Key"`
		VersionID string `xml:"VersionId"`
		ETag      string `xml:"ETag"` // delete only if it matches
	} `xml:"Object"`
}

type deletedXML struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
}

type deleteErrorXML struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type deleteResult struct {
	XMLName xml.Name         `xml:"DeleteResult"`
	NS      string           `xml:"xmlns,attr"`
	Deleted []deletedXML     `xml:"Deleted"`
	Errors  []deleteErrorXML `xml:"Error"`
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	NS       string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// checksumsXML are the Checksum<ALGO> elements of parts and results.
type checksumsXML struct {
	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
}

func (c *checksumsXML) setChecksum(algo, v string) {
	switch algo {
	case "CRC32":
		c.ChecksumCRC32 = v
	case "CRC32C":
		c.ChecksumCRC32C = v
	case "CRC64NVME":
		c.ChecksumCRC64NVME = v
	case "SHA1":
		c.ChecksumSHA1 = v
	case "SHA256":
		c.ChecksumSHA256 = v
	}
}

// checksum returns the value for algo, or the only value given if algo is
// empty.
func (c *checksumsXML) checksum(algo string) string {
	all := map[string]string{"CRC32": c.ChecksumCRC32, "CRC32C": c.ChecksumCRC32C, "CRC64NVME": c.ChecksumCRC64NVME,
		"SHA1": c.ChecksumSHA1, "SHA256": c.ChecksumSHA256}
	if algo != "" {
		return all[algo]
	}
	return ""
}

type completeMultipartUpload struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
		checksumsXML
	} `xml:"Part"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	NS       string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
	checksumsXML
	ChecksumType string `xml:"ChecksumType,omitempty"`
}

type partXML struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	checksumsXML
}

type listPartsResult struct {
	XMLName              xml.Name  `xml:"ListPartsResult"`
	NS                   string    `xml:"xmlns,attr"`
	Bucket               string    `xml:"Bucket"`
	Key                  string    `xml:"Key"`
	UploadID             string    `xml:"UploadId"`
	Initiator            owner     `xml:"Initiator"`
	Owner                owner     `xml:"Owner"`
	StorageClass         string    `xml:"StorageClass"`
	PartNumberMarker     int       `xml:"PartNumberMarker"`
	NextPartNumberMarker int       `xml:"NextPartNumberMarker"`
	MaxParts             int       `xml:"MaxParts"`
	IsTruncated          bool      `xml:"IsTruncated"`
	ChecksumAlgorithm    string    `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumType         string    `xml:"ChecksumType,omitempty"`
	Parts                []partXML `xml:"Part"`
}

type uploadXML struct {
	Key          string `xml:"Key"`
	UploadID     string `xml:"UploadId"`
	Initiator    owner  `xml:"Initiator"`
	Owner        owner  `xml:"Owner"`
	StorageClass string `xml:"StorageClass"`
	Initiated    string `xml:"Initiated"`
}

type listMultipartUploadsResult struct {
	XMLName            xml.Name          `xml:"ListMultipartUploadsResult"`
	NS                 string            `xml:"xmlns,attr"`
	Bucket             string            `xml:"Bucket"`
	KeyMarker          string            `xml:"KeyMarker"`
	UploadIDMarker     string            `xml:"UploadIdMarker"`
	NextKeyMarker      string            `xml:"NextKeyMarker"`
	NextUploadIDMarker string            `xml:"NextUploadIdMarker"`
	Delimiter          string            `xml:"Delimiter,omitempty"`
	Prefix             string            `xml:"Prefix"`
	EncodingType       string            `xml:"EncodingType,omitempty"`
	MaxUploads         int               `xml:"MaxUploads"`
	IsTruncated        bool              `xml:"IsTruncated"`
	Uploads            []uploadXML       `xml:"Upload"`
	CommonPrefixes     []commonPrefixXML `xml:"CommonPrefixes"`
}

type accessControlPolicy struct {
	XMLName xml.Name `xml:"AccessControlPolicy"`
	NS      string   `xml:"xmlns,attr"`
	Owner   owner    `xml:"Owner"`
	Grants  []grant  `xml:"AccessControlList>Grant"`
}

type grant struct {
	Grantee struct {
		XMLNS       string `xml:"xmlns:xsi,attr"`
		Type        string `xml:"xsi:type,attr"`
		ID          string `xml:"ID"`
		DisplayName string `xml:"DisplayName"`
	} `xml:"Grantee"`
	Permission string `xml:"Permission"`
}

// maxXMLBody bounds request bodies that are parsed as XML.
const maxXMLBody = 2 << 20

// readXML decodes a small XML request body.
func readXML(r io.Reader, v any) error {
	data, err := io.ReadAll(io.LimitReader(r, maxXMLBody+1))
	if err != nil {
		return err
	}
	if len(data) > maxXMLBody {
		return s3err.MalformedXML.With("The XML you provided was larger than the maximum allowed")
	}
	if len(data) == 0 {
		return s3err.MissingRequestBody
	}
	if err := xml.Unmarshal(data, v); err != nil {
		return s3err.MalformedXML
	}
	return nil
}

// writeXML sends an XML response.
func writeXML(w http.ResponseWriter, status int, v any) {
	out, err := xml.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	w.Write([]byte(xml.Header))
	w.Write(out)
}

type objectAttributesXML struct {
	XMLName      xml.Name        `xml:"GetObjectAttributesResponse"`
	NS           string          `xml:"xmlns,attr"`
	ETag         string          `xml:"ETag,omitempty"`
	Checksum     *checksumAttrs  `xml:"Checksum,omitempty"`
	ObjectParts  *objectPartsXML `xml:"ObjectParts,omitempty"`
	StorageClass string          `xml:"StorageClass,omitempty"`
	ObjectSize   *int64          `xml:"ObjectSize,omitempty"`
}

type checksumAttrs struct {
	checksumsXML
	ChecksumType string `xml:"ChecksumType,omitempty"`
}

type objectPartsXML struct {
	TotalPartsCount      int             `xml:"PartsCount"`
	PartNumberMarker     int             `xml:"PartNumberMarker"`
	NextPartNumberMarker int             `xml:"NextPartNumberMarker"`
	MaxParts             int             `xml:"MaxParts"`
	IsTruncated          bool            `xml:"IsTruncated"`
	Parts                []objectPartXML `xml:"Part"`
}

type objectPartXML struct {
	PartNumber int   `xml:"PartNumber"`
	Size       int64 `xml:"Size"`
	checksumsXML
}
