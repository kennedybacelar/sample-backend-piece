import os
import zipfile
from datetime import date
from typing import Optional, cast
import json
import rsa
from pydantic import BaseModel
from structlog import get_logger
from gitential2.utils import find_first


logger = get_logger(__name__)


class LicenseError(Exception):
    pass


class License(BaseModel):
    license_id: int
    customer_name: str
    end_date: date
    is_on_premises: bool
    number_of_developers: int

    def is_valid(self):
        return self.end_date >= date.today()

    @property
    def is_cloud(self):
        return not self.is_on_premises

    @property
    def installation_type(self):
        return "on-prem" if self.is_on_premises else "cloud"

    def as_config(self):
        return {
            "valid_until": int(self.end_date.strftime("%s")),
            "customer_name": self.customer_name,
            "installation_type": self.installation_type,
            "number_of_developers": self.number_of_developers,
        }


unknown_customer = License(
    license_id=-1,
    customer_name="UNREGISTERED",
    end_date=date(1970, 1, 1),
    is_on_premises=True,
    number_of_developers=100,
)

_LICENSE = None

dummy_license = License(
    license_id=-2,
    customer_name="DUMMY",
    end_date=date(2099, 12, 31),
    is_on_premises=True,
    number_of_developers=100,
)


def check_license(license_file_path: Optional[str] = None) -> License:
    global _LICENSE  # pylint: disable=global-statement
    if _LICENSE is None:
        try:
            _LICENSE = load_license(license_file_path=license_file_path)
        except:  # pylint: disable=bare-except
            logger.exception("Failed to load license file.")
            return unknown_customer
    return _LICENSE


def load_license(license_file_path: Optional[str] = None, bits=2048) -> License:  # pylint: disable=too-complex
    license_file_path = license_file_path or os.environ.get("GITENTIAL_LICENSE", "/etc/gitential/license/license.bin")

    with zipfile.ZipFile(cast(str, license_file_path), "r", compression=zipfile.ZIP_DEFLATED) as _zip:
        name_list = _zip.namelist()
        # print(name_list)
        if len(name_list) > 4:
            raise LicenseError("001")
        lic_file = find_first(lambda name: name.endswith(".lic"), name_list)
        if not lic_file:
            raise LicenseError("002")

        try:
            name, _ = lic_file.split(".")
            client_slug, license_id = name.split("_")
        except:
            raise LicenseError("003")  # pylint: disable=raise-missing-from

        # gitential_public_key_bytes = _zip.read("GITENTIAL-public.key")
        # client_public_key_bytes = _zip.read(f"{client_slug}-public.key")

        # pylint: disable=consider-using-with
        gitential_public_key_bytes = open(
            os.path.dirname(os.path.abspath(__file__)) + "/GITENTIAL-public.pem", "rb"
        ).read()

        client_private_key_bytes = _zip.read(f"{client_slug}-private.key")
        license_file_bytes = _zip.read(lic_file)

        private_key = cast(rsa.PrivateKey, rsa.PrivateKey.load_pkcs1(client_private_key_bytes))
        gitential_public_key = cast(rsa.PublicKey, rsa.PublicKey.load_pkcs1(gitential_public_key_bytes))

        signature_bytes, licence_bytes = license_file_bytes[-int(bits / 8) :], license_file_bytes[0 : -int(bits / 8)]

        try:
            licence = rsa.decrypt(licence_bytes, private_key)
        except rsa.DecryptionError:
            raise LicenseError("004")  # pylint: disable=raise-missing-from

        try:
            rsa.verify(licence, signature_bytes, gitential_public_key)
        except rsa.VerificationError:
            raise LicenseError("005")  # pylint: disable=raise-missing-from
        try:
            license_content = json.loads(licence)
        except json.JSONDecodeError:
            raise LicenseError("006")  # pylint: disable=raise-missing-from
        try:
            return License(
                license_id=int(license_id),
                customer_name=license_content["C"],
                end_date=license_content["E"],
                is_on_premises=license_content["O"] == "1",
                number_of_developers=int(license_content["D"]),
            )
        except:
            raise LicenseError("007")  # pylint: disable=raise-missing-from


def is_on_prem_installation(license_file_path: Optional[str] = None) -> bool:
    gitential_license: License = check_license(license_file_path)
    return gitential_license.is_on_premises if gitential_license is not None else False
