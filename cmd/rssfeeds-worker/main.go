// Command rssfeeds-worker is the official RSS Manager backend (public SDK only).
package main

import (
	"bufio"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

type feed struct {
	URL             string `json:"url"`
	Label           string `json:"label,omitempty"`
	Enabled         bool   `json:"enabled"`
	LoginAccount    string `json:"loginAccount,omitempty"`
	IncludeRegex    string `json:"includeRegex,omitempty"`
	ExcludeRegex    string `json:"excludeRegex,omitempty"`
	IntervalSec     int    `json:"intervalSec,omitempty"`
	GatewayID       string `json:"gatewayId,omitempty"`
	LastOk          string `json:"lastOk,omitempty"`
	LastError       string `json:"lastError,omitempty"`
	ItemsSeen       int    `json:"itemsSeen,omitempty"`
	LastMatchedRule string `json:"lastMatchedRule,omitempty"`
	LastPollUnix    int64  `json:"lastPollUnix,omitempty"`
}

type feedStore struct {
	Feeds []feed `json:"feeds"`
}

type rss struct {
	XMLName xml.Name   `xml:"rss"`
	Channel rssChannel `xml:"channel"`
}

type rssChannel struct {
	Items []rssItem `xml:"item"`
}

type rssItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	Description string `xml:"description"`
	Enclosure   struct {
		URL string `xml:"url,attr"`
	} `xml:"enclosure"`
}

type candidate struct {
	Title  string
	Magnet string
}

type rewriteResult struct {
	URL         string
	MatchedRule string
}

var (
	mu       sync.Mutex
	nextHost = int64(1)
	pending  = map[int64]chan hostReply{}
	stdinMu  sync.Mutex
	pollMu   sync.Mutex
)

type hostReply struct {
	result json.RawMessage
	err    string
}

func main() {
	go loopPoll()
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for in.Scan() {
		line := append([]byte(nil), in.Bytes()...)
		if len(line) == 0 {
			continue
		}
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(line, &probe); err != nil {
			continue
		}
		if _, hasMethod := probe["method"]; !hasMethod {
			var resp struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idNum, ok := asInt64(resp.ID)
			if !ok {
				continue
			}
			mu.Lock()
			ch := pending[idNum]
			delete(pending, idNum)
			mu.Unlock()
			if ch == nil {
				continue
			}
			msg := ""
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			ch <- hostReply{result: resp.Result, err: msg}
			continue
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		// Concurrent so hostCall reverse-RPC can complete while stdin keeps reading replies.
		go handleRequest(req)
	}
}

func handleRequest(req request) {
	switch req.Method {
	case "fleet.handshake", "fleet.ping", "ext.activate", "ext.deactivate", "ext.health", "ext.upgrade":
		reply(req.ID, map[string]any{"ok": true})
	case "fleet.shutdown":
		reply(req.ID, map[string]any{"ok": true})
		os.Exit(0)
	case "ext.rssfeeds.list":
		st, err := loadFeeds()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, st)
	case "ext.rssfeeds.add":
		var p struct {
			URL          string `json:"url"`
			Label        string `json:"label"`
			LoginAccount string `json:"loginAccount"`
			IncludeRegex string `json:"includeRegex"`
			ExcludeRegex string `json:"excludeRegex"`
			IntervalSec  int    `json:"intervalSec"`
			GatewayID    string `json:"gatewayId"`
			Enabled      *bool  `json:"enabled"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil || strings.TrimSpace(p.URL) == "" {
			replyErr(req.ID, "url required")
			return
		}
		st, err := loadFeeds()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		enabled := true
		if p.Enabled != nil {
			enabled = *p.Enabled
		}
		f := normalizeFeed(feed{
			URL:          strings.TrimSpace(p.URL),
			Label:        strings.TrimSpace(p.Label),
			Enabled:      enabled,
			LoginAccount: strings.TrimSpace(p.LoginAccount),
			IncludeRegex: strings.TrimSpace(p.IncludeRegex),
			ExcludeRegex: strings.TrimSpace(p.ExcludeRegex),
			IntervalSec:  p.IntervalSec,
			GatewayID:    strings.TrimSpace(p.GatewayID),
		})
		if errMsg := validateFeedFilters(f); errMsg != "" {
			replyErr(req.ID, errMsg)
			return
		}
		replaced := false
		for i := range st.Feeds {
			if st.Feeds[i].URL == f.URL {
				f.LastOk = st.Feeds[i].LastOk
				f.LastError = st.Feeds[i].LastError
				f.ItemsSeen = st.Feeds[i].ItemsSeen
				f.LastMatchedRule = st.Feeds[i].LastMatchedRule
				f.LastPollUnix = st.Feeds[i].LastPollUnix
				st.Feeds[i] = f
				replaced = true
				break
			}
		}
		if !replaced {
			st.Feeds = append(st.Feeds, f)
		}
		if err := saveFeeds(st); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, st)
	case "ext.rssfeeds.poll":
		added, err := pollFeeds(true)
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		st, _ := loadFeeds()
		reply(req.ID, map[string]any{"ok": true, "added": added, "feeds": st.Feeds})
	default:
		replyErr(req.ID, fmt.Sprintf("method not found: %s", req.Method))
	}
}

func loopPoll() {
	time.Sleep(5 * time.Second)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		_, _ = pollFeeds(false)
	}
}

func pollFeeds(force bool) (int, error) {
	pollMu.Lock()
	defer pollMu.Unlock()

	st, err := loadFeeds()
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	added := 0
	changed := false
	for i := range st.Feeds {
		f := &st.Feeds[i]
		*f = normalizeFeed(*f)
		if !f.Enabled {
			continue
		}
		interval := normalizeInterval(f.IntervalSec)
		if !force && f.LastPollUnix > 0 && now.Unix()-f.LastPollUnix < int64(interval) {
			continue
		}
		n, err := pollOne(f, now)
		changed = true
		if err != nil {
			f.LastError = sanitizeDiag(err.Error())
			f.LastOk = ""
			continue
		}
		added += n
	}
	if changed {
		if err := saveFeeds(st); err != nil {
			return added, err
		}
	}
	return added, nil
}

func pollOne(f *feed, now time.Time) (int, error) {
	f.LastPollUnix = now.Unix()
	if acc := strings.TrimSpace(f.LoginAccount); acc != "" {
		if _, err := hostCall("fleet.loginmgr.login", map[string]any{"name": acc}); err != nil {
			return 0, fmt.Errorf("login %s: %w", acc, err)
		}
	}
	body, err := hostCall("fleet.http.fetch", map[string]any{"url": f.URL})
	if err != nil {
		return 0, fmt.Errorf("fetch: %w", err)
	}
	var fetched struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	if err := json.Unmarshal(body, &fetched); err != nil {
		return 0, fmt.Errorf("fetch decode: %w", err)
	}
	if fetched.Status == 401 || fetched.Status == 403 {
		return 0, fmt.Errorf("auth failed (HTTP %d)", fetched.Status)
	}
	if fetched.Status >= 400 {
		return 0, fmt.Errorf("HTTP %d", fetched.Status)
	}

	cands := extractCandidates(fetched.Body)
	f.ItemsSeen = len(cands)
	magnets := make([]string, 0, len(cands))
	lastRule := ""
	for _, c := range cands {
		ok, errMsg := itemAllowed(c.Title, f.IncludeRegex, f.ExcludeRegex)
		if errMsg != "" {
			return 0, fmt.Errorf("%s", errMsg)
		}
		if !ok {
			continue
		}
		rew := rewriteURL(c.Magnet)
		if rew.MatchedRule != "" {
			lastRule = rew.MatchedRule
		}
		m := rew.URL
		if !strings.HasPrefix(strings.ToLower(m), "magnet:?") {
			continue
		}
		magnets = append(magnets, m)
	}
	if lastRule != "" {
		f.LastMatchedRule = lastRule
	}
	if len(magnets) == 0 {
		f.LastOk = now.Format(time.RFC3339)
		f.LastError = ""
		return 0, nil
	}

	if err := enqueueMagnets(magnets, f); err != nil {
		return 0, err
	}
	f.LastOk = now.Format(time.RFC3339)
	f.LastError = ""
	return len(magnets), nil
}

func enqueueMagnets(magnets []string, f *feed) error {
	gatewayID := strings.TrimSpace(f.GatewayID)
	if gatewayID != "" {
		if _, err := hostCall("fleet.intake.add", map[string]any{
			"magnets":   magnets,
			"start":     true,
			"gatewayId": gatewayID,
		}); err != nil {
			return fmt.Errorf("intake: %w", err)
		}
	} else {
		if _, err := hostCall("fleet.torrents.add", map[string]any{
			"magnets": magnets,
			"start":   true,
		}); err != nil {
			return fmt.Errorf("add: %w", err)
		}
	}
	label := strings.TrimSpace(f.Label)
	if label == "" {
		return nil
	}
	for _, m := range magnets {
		hash := magnetInfoHash(m)
		if hash == "" {
			continue
		}
		_, _ = hostCall("fleet.torrents.setLabel", map[string]any{"hash": hash, "label": label})
	}
	return nil
}

func extractCandidates(body string) []candidate {
	var doc rss
	_ = xml.Unmarshal([]byte(body), &doc)
	seen := map[string]struct{}{}
	out := []candidate{}
	add := func(title, raw string) {
		raw = strings.TrimSpace(raw)
		if !strings.HasPrefix(strings.ToLower(raw), "magnet:?") {
			return
		}
		if _, ok := seen[raw]; ok {
			return
		}
		seen[raw] = struct{}{}
		out = append(out, candidate{Title: strings.TrimSpace(title), Magnet: raw})
	}
	for _, item := range doc.Channel.Items {
		add(item.Title, item.Link)
		add(item.Title, item.Enclosure.URL)
		for _, part := range strings.Fields(item.Description) {
			if strings.HasPrefix(strings.ToLower(part), "magnet:?") {
				add(item.Title, part)
			}
		}
	}
	lower := strings.ToLower(body)
	scan := body
	for {
		i := strings.Index(lower, "magnet:?")
		if i < 0 {
			break
		}
		rest := scan[i:]
		end := strings.IndexAny(rest, "\"'<> \n\r\t")
		var m string
		if end < 0 {
			m = rest
			add("", m)
			break
		}
		m = rest[:end]
		add("", m)
		scan = scan[i+end:]
		lower = strings.ToLower(scan)
	}
	return out
}

func rewriteURL(u string) rewriteResult {
	u = strings.TrimSpace(u)
	if u == "" {
		return rewriteResult{URL: u}
	}
	raw, err := hostCall("fleet.urlrewrite.apply", map[string]any{"url": u})
	if err != nil {
		return rewriteResult{URL: u}
	}
	var res struct {
		URL         string `json:"url"`
		MatchedRule string `json:"matchedRule"`
	}
	if err := json.Unmarshal(raw, &res); err != nil || strings.TrimSpace(res.URL) == "" {
		return rewriteResult{URL: u}
	}
	return rewriteResult{URL: strings.TrimSpace(res.URL), MatchedRule: strings.TrimSpace(res.MatchedRule)}
}

func normalizeFeed(f feed) feed {
	f.URL = strings.TrimSpace(f.URL)
	f.Label = strings.TrimSpace(f.Label)
	f.LoginAccount = strings.TrimSpace(f.LoginAccount)
	f.IncludeRegex = strings.TrimSpace(f.IncludeRegex)
	f.ExcludeRegex = strings.TrimSpace(f.ExcludeRegex)
	f.GatewayID = strings.TrimSpace(f.GatewayID)
	f.IntervalSec = normalizeInterval(f.IntervalSec)
	return f
}

func validateFeedFilters(f feed) string {
	if f.IncludeRegex != "" {
		if _, e := itemAllowed("probe", f.IncludeRegex, ""); e != "" {
			return e
		}
	}
	if f.ExcludeRegex != "" {
		if _, e := itemAllowed("probe", "", f.ExcludeRegex); e != "" {
			return e
		}
	}
	return ""
}

func sanitizeDiag(msg string) string {
	msg = strings.TrimSpace(msg)
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "cookie") {
		return "authentication or session error (details omitted)"
	}
	if len(msg) > 240 {
		return msg[:240]
	}
	return msg
}

func loadFeeds() (*feedStore, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "feeds"})
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" || len(raw) == 0 {
		return &feedStore{Feeds: []feed{}}, nil
	}
	var st feedStore
	if err := json.Unmarshal(raw, &st); err != nil {
		return &feedStore{Feeds: []feed{}}, nil
	}
	if st.Feeds == nil {
		st.Feeds = []feed{}
	}
	return &st, nil
}

func saveFeeds(st *feedStore) error {
	if st.Feeds == nil {
		st.Feeds = []feed{}
	}
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "feeds", "value": st})
	return err
}

func hostCall(method string, params any) (json.RawMessage, error) {
	mu.Lock()
	id := nextHost
	nextHost++
	ch := make(chan hostReply, 1)
	pending[id] = ch
	mu.Unlock()
	req := map[string]any{"jsonrpc": "2.0", "method": method, "params": params, "id": id}
	line, _ := json.Marshal(req)
	line = append(line, '\n')
	stdinMu.Lock()
	_, err := os.Stdout.Write(line)
	stdinMu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case rep := <-ch:
		if rep.err != "" {
			return nil, fmt.Errorf("%s", rep.err)
		}
		return rep.result, nil
	case <-time.After(30 * time.Second):
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, fmt.Errorf("host call timeout")
	}
}

func reply(id any, result any) {
	stdinMu.Lock()
	defer stdinMu.Unlock()
	enc := json.NewEncoder(os.Stdout)
	_ = enc.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func replyErr(id any, msg string) {
	stdinMu.Lock()
	defer stdinMu.Unlock()
	enc := json.NewEncoder(os.Stdout)
	_ = enc.Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   map[string]any{"code": -32000, "message": msg},
	})
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}
