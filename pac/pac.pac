function FindProxyForURL(url, host) {

  if (dnsDomainIs(host, "mapcamera.com") || shExpMatch(host, "*.mapcamera.com")) {
    return "PROXY gw.dataimpulse.com:823";
  }

  if (dnsDomainIs(host, "cman.jp") || shExpMatch(host, "*.cman.jp")) {
    return "PROXY gw.dataimpulse.com:823";
  }

  return "DIRECT";
}
