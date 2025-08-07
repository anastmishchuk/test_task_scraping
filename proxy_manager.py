import requests
import logging


class ProxyManager:
    def __init__(self, config):
        self.config = config
        self.current_proxy_index = 0
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        self.setup_proxy_session()

    def setup_proxy_session(self):
        proxy_config = self.config.get("proxy", {})
        if not proxy_config.get("enabled", False):
            self.logger.info("Proxy disabled")
            return

        try:
            proxy_info = self.get_current_proxy()
            if proxy_info:
                self.session.proxies.update(proxy_info)
                self.logger.info(f"Proxy configured: {proxy_info}")
            else:
                self.logger.warning(
                    "Proxy enabled but no valid proxy configuration found"
                )
        except Exception as e:
            self.logger.error(f"Error setting up proxy: {e}")

    def get_current_proxy(self):
        proxy_config = self.config.get("proxy", {})

        if not proxy_config.get("enabled", False):
            return None

        if proxy_config.get("rotate_proxies", False):
            proxy_list = proxy_config.get("proxy_list", [])
            if isinstance(proxy_list, list) and proxy_list:
                current_proxy = proxy_list[self.current_proxy_index % len(proxy_list)]
                return self.format_proxy_dict(current_proxy)
            else:
                self.logger.warning(
                    "Proxy rotation is enabled, but 'proxy_list' is missing or invalid."
                )

        if proxy_config.get("host"):
            return self.format_proxy_dict(proxy_config)

        return None

    def format_proxy_dict(self, proxy_config):
        try:
            proxy_type = proxy_config.get("type", "http").lower()
            host = proxy_config.get("host", "")
            port = proxy_config.get("port", 8080)
            username = proxy_config.get("username", "")
            password = proxy_config.get("password", "")

            if not host:
                return None

            if username and password:
                proxy_url = f"{proxy_type}://{username}:{password}@{host}:{port}"
            else:
                proxy_url = f"{proxy_type}://{host}:{port}"

            if proxy_type in ["http", "https", "socks5", "socks4"]:
                return {"http": proxy_url, "https": proxy_url}
            else:
                self.logger.warning(f"Unsupported proxy type: {proxy_type}")
                return None

        except Exception as e:
            self.logger.error(f"Error formatting proxy: {e}")
            return None

    def rotate_proxy(self):
        proxy_config = self.config.get("proxy", {})
        if proxy_config.get("rotate_proxies", False):
            proxy_list = proxy_config.get("proxy_list", [])
            if proxy_list:
                self.current_proxy_index = (self.current_proxy_index + 1) % len(proxy_list)
                self.setup_proxy_session()
                self.logger.info(f"Rotated to proxy index: {self.current_proxy_index}")

    def get_session(self):
        return self.session

    def get_proxy_for_selenium(self):
        proxy_config = self.config.get("proxy", {})
        if not proxy_config.get("enabled", False):
            return None

        proxy_info = self.get_current_proxy()
        if proxy_info:
            proxy_url = proxy_info.get("http", "")
            return proxy_url
        return None
