package peer

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
)

// IP returns the public IP address of the local machine. This function will
// make multiple HTTPS requests to multiple APIs and return the IP address that
// is reported by the majority.
func IP() (string, error) {
	urls := []string{
		"https://ipv4bot.whatismyipaddress.com",
		"https://api.ipify.org/?format=text",
		"https://ipapi.co/ip",
	}

	ipsMu := new(sync.Mutex)
	ips := []string{}

	for _, url := range urls {
		go func(url string) {
			resp, err := http.Get(url)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			raw, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return
			}

			ipsMu.Lock()
			ips = append(ips, string(raw))
			ipsMu.Unlock()
		}(url)
	}

	threshold := len(ips)/2 + 1
	votes := map[string]int{}
	for _, ip := range ips {
		votes[ip] = votes[ip] + 1
	}
	for ip, n := range votes {
		if n >= threshold {
			return ip, nil
		}
	}
	return "", fmt.Errorf("no ip address found")
}
