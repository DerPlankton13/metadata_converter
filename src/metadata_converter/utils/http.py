import requests

DEFAULT_USER_AGENT = "metadata-collector/1.0"


def make_session(user_agent: str = DEFAULT_USER_AGENT, max_redirects: int = 0) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    session.max_redirects = max_redirects
    return session