package imageprepull

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

const ociIndexMediaType = "application/vnd.oci.image.index.v1+json"
const ociManifestMediaType = "application/vnd.oci.image.manifest.v1+json"

// ApprovedRuntimeDigests verifies exact OCI index bytes against the approved
// immutable image reference, then checks its unique Linux amd64/arm64 children
// against independently reviewed CI platform digests. It accepts BuildKit's
// explicitly linked unknown/unknown attestation descriptors but never treats
// them as runnable images. This is KubeBrain's two-platform release policy, not
// a general OCI resolver. Nested indexes and unverified platform requirements
// are rejected instead of guessing the runtime's selection.
//
// The caller must authenticate the CI release evidence and its source revision;
// fields supplied by an arbitrary JSON receipt are not release authorization.
// This function does not fetch child manifests/layers, verify signatures or
// attestations, or prove binary behavior. Its result permits both child and
// index runtime imageIDs for each verified platform (including CRI-O's form).
func ApprovedRuntimeDigests(image string, rawIndex []byte, reviewed map[string]string) (map[string][]string, error) {
	if len(image) > 2048 || !pinnedImage.MatchString(image) || strings.ContainsFunc(image, unicode.IsControl) || len(rawIndex) == 0 || len(rawIndex) > 1<<20 || !utf8.Valid(rawIndex) {
		return nil, errors.New("release requires a pinned image and bounded UTF-8 OCI index bytes")
	}
	_, digest, _ := strings.Cut(image, "@")
	actualDigest := fmt.Sprintf("sha256:%x", sha256.Sum256(rawIndex))
	if digest != actualDigest {
		return nil, errors.New("OCI index bytes do not match the approved image digest")
	}
	if len(reviewed) != 2 || !validDigest(reviewed["linux/amd64"]) || !validDigest(reviewed["linux/arm64"]) || reviewed["linux/amd64"] == reviewed["linux/arm64"] {
		return nil, errors.New("release lacks two distinct reviewed Linux platform manifest digests")
	}
	if err := uniqueJSON(rawIndex); err != nil {
		return nil, err
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(rawIndex, &root); err != nil {
		return nil, err
	}
	if err := exactFieldNames(root, "schemaVersion", "mediaType", "manifests", "artifactType", "subject"); err != nil {
		return nil, err
	}
	var schema int
	var media string
	var manifests []json.RawMessage
	if json.Unmarshal(root["schemaVersion"], &schema) != nil || schema != 2 ||
		json.Unmarshal(root["mediaType"], &media) != nil || media != ociIndexMediaType ||
		json.Unmarshal(root["manifests"], &manifests) != nil || len(manifests) < 2 || len(manifests) > 4 ||
		root["artifactType"] != nil || root["subject"] != nil {
		return nil, errors.New("release must be a bounded OCI image index, not an artifact or nested index")
	}
	result := map[string][]string{}
	seenDigests, attestations := map[string]bool{}, map[string]bool{}
	for _, raw := range manifests {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, err
		}
		if err := exactFieldNames(fields, "mediaType", "digest", "size", "platform", "annotations", "urls", "data", "artifactType"); err != nil {
			return nil, err
		}
		var childMedia, childDigest string
		var size int64
		var platform map[string]json.RawMessage
		var osName, arch, variant, osVersion string
		var features, osFeatures []string
		var annotations map[string]string
		if json.Unmarshal(fields["mediaType"], &childMedia) != nil || childMedia != ociManifestMediaType ||
			json.Unmarshal(fields["digest"], &childDigest) != nil || !validDigest(childDigest) || seenDigests[childDigest] || childDigest == digest ||
			json.Unmarshal(fields["size"], &size) != nil || size <= 0 || size > 1<<20 ||
			json.Unmarshal(fields["platform"], &platform) != nil ||
			json.Unmarshal(platform["os"], &osName) != nil || json.Unmarshal(platform["architecture"], &arch) != nil ||
			optionalJSON(platform, "variant", &variant) != nil || optionalJSON(platform, "os.version", &osVersion) != nil ||
			optionalJSON(platform, "features", &features) != nil || optionalJSON(platform, "os.features", &osFeatures) != nil ||
			optionalJSON(fields, "annotations", &annotations) != nil || fields["urls"] != nil || fields["data"] != nil || fields["artifactType"] != nil {
			return nil, errors.New("release has an invalid, duplicate, or unsupported manifest descriptor")
		}
		seenDigests[childDigest] = true
		if err := exactFieldNames(platform, "os", "architecture", "variant", "os.version", "features", "os.features"); err != nil {
			return nil, err
		}
		if osVersion != "" || len(features) != 0 || len(osFeatures) != 0 ||
			(variant != "" && !(arch == "arm64" && variant == "v8") && !(arch == "amd64" && variant == "v1")) {
			return nil, errors.New("release requires unverified OS or CPU features")
		}
		if osName == "unknown" && arch == "unknown" {
			ref := annotations["vnd.docker.reference.digest"]
			if annotations["vnd.docker.reference.type"] != "attestation-manifest" || attestations[ref] ||
				(ref != reviewed["linux/amd64"] && ref != reviewed["linux/arm64"]) {
				return nil, errors.New("release attestation descriptor is not uniquely linked to a reviewed platform")
			}
			attestations[ref] = true
			continue
		}
		platformName := osName + "/" + arch
		if reviewed[platformName] != childDigest || result[platformName] != nil || annotations["vnd.docker.reference.type"] != "" {
			return nil, errors.New("release platform selection is ambiguous or differs from reviewed CI evidence")
		}
		result[platformName] = []string{childDigest, digest}
	}
	if len(result) != 2 {
		return nil, errors.New("release does not provide both reviewed Linux platforms")
	}
	return result, nil
}

func validDigest(digest string) bool {
	return len(digest) == 71 && pinnedImage.MatchString("image@"+digest)
}

func optionalJSON(object map[string]json.RawMessage, key string, target any) error {
	if raw, ok := object[key]; ok {
		return json.Unmarshal(raw, target)
	}
	return nil
}

func exactFieldNames(object map[string]json.RawMessage, fields ...string) error {
	for key := range object {
		for _, field := range fields {
			if key != field && strings.EqualFold(key, field) {
				return errors.New("release JSON uses a noncanonical identity field name")
			}
		}
	}
	return nil
}

// Reject duplicate (including case-aliased) object keys before typed field
// extraction. Otherwise different consumers may select different descriptors.
func uniqueJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 32 {
			return errors.New("release JSON exceeds its nesting bound")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, isDelimiter := token.(json.Delim)
		if !isDelimiter {
			return nil
		}
		switch delimiter {
		case '{':
			keys := map[string]bool{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok || keys[strings.ToLower(key)] {
					return errors.New("release JSON contains duplicate or case-aliased fields")
				}
				keys[strings.ToLower(key)] = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		default:
			return errors.New("release JSON contains an unexpected delimiter")
		}
		_, err = decoder.Token()
		return err
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("release JSON has trailing data")
	}
	return nil
}
