import json
from cryptography.fernet import Fernet as CryptoFernet
from gitential2.settings import GitentialSettings


class Fernet:
    def __init__(self, settings: GitentialSettings):
        self.f = CryptoFernet(settings.fernet_key)

    def encrypt_string(self, s: str) -> str:
        return self.f.encrypt(s.encode()).decode()

    def decrypt_string(self, s: str) -> str:
        return self.f.decrypt(s.encode()).decode()

    def encrypt_bytes(self, b: bytes) -> bytes:
        return self.f.encrypt(b)

    def decrypt_bytes(self, b: bytes) -> bytes:
        return self.f.decrypt(b)


class FernetVault:
    def __init__(self, secret_key: bytes):
        self.f = CryptoFernet(secret_key)
        self._vault: dict = {}

    def __getitem__(self, key):
        return self._vault.__getitem__(key)

    def __setitem__(self, key, value):
        return self._vault.__setitem__(key, value)

    def __delitem__(self, key):
        return self._vault.__delitem__(key)

    def load(self, path):
        with open(path, "r", encoding="utf-8") as f:
            contents = f.read()
            self._vault = {self.decrypt_string(k): self.decrypt_string(v) for k, v in json.loads(contents).items()}

    def save(self, path):
        with open(path, "w", encoding="utf-8") as f:
            encrypted_vault = {self.encrypt_string(k): self.encrypt_string(v) for k, v in self._vault.items()}
            json_vault = json.dumps(encrypted_vault)
            f.write(json_vault)

    def encrypt_string(self, s: str) -> str:
        return self.f.encrypt(s.encode()).decode()

    def decrypt_string(self, s: str) -> str:
        return self.f.decrypt(s.encode()).decode()
