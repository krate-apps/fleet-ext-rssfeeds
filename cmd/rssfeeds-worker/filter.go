package main

import (
	"regexp"
	"strings"
)

func normalizeInterval(sec int) int {
	if sec <= 0 {
		return 300
	}
	if sec < 60 {
		return 60
	}
	return sec
}

func itemAllowed(title, includeRegex, excludeRegex string) (bool, string) {
	title = strings.TrimSpace(title)
	inc := strings.TrimSpace(includeRegex)
	exc := strings.TrimSpace(excludeRegex)
	if inc != "" {
		re, err := regexp.Compile(inc)
		if err != nil {
			return false, "invalid includeRegex"
		}
		if !re.MatchString(title) {
			return false, ""
		}
	}
	if exc != "" {
		re, err := regexp.Compile(exc)
		if err != nil {
			return false, "invalid excludeRegex"
		}
		if re.MatchString(title) {
			return false, ""
		}
	}
	return true, ""
}

func magnetInfoHash(magnet string) string {
	lower := strings.ToLower(magnet)
	const marker = "xt=urn:btih:"
	i := strings.Index(lower, marker)
	if i < 0 {
		return ""
	}
	rest := magnet[i+len(marker):]
	end := strings.IndexAny(rest, "&")
	if end >= 0 {
		rest = rest[:end]
	}
	h := strings.TrimSpace(rest)
	if len(h) != 40 && len(h) != 32 {
		return ""
	}
	return strings.ToUpper(h)
}
