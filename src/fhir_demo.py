"""fhir_demo.py: create / update clearly-labelled TEST patients on the public HAPI sandbox."""
import requests

MARITAL = "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus"


def _headers(cfg):
    ct = cfg["api"]["accept_headers"]["json"]
    return {"Accept": ct, "Content-Type": ct, "Prefer": "return=representation"}


def _summary(p, response):
    return {"id": p["id"], "version": p["meta"]["versionId"],
            "last_updated": p["meta"]["lastUpdated"], "http_status": response.status_code}


def create_test_patient(cfg, family, given, city, run_tag):
    body = {
        "resourceType": "Patient",
        "identifier": [{"system": "urn:scd2-demo", "value": run_tag}],
        "active": True,
        "name": [{"family": family, "given": [given]}],
        "gender": "female",
        "birthDate": "1990-01-15",
        "maritalStatus": {"coding": [{"system": MARITAL, "code": "S", "display": "Never Married"}]},
        "address": [{"city": city, "state": "MH", "country": "IN"}],
    }
    r = requests.post(f"{cfg['api']['base_url']}/Patient", json=body,
                      headers=_headers(cfg), timeout=cfg["api"]["timeout_seconds"])
    r.raise_for_status()
    return _summary(r.json(), r)


def update_test_patient(cfg, patient_id, mutate):
    """Read the current resource, apply mutate(resource), PUT it back (creates a new version)."""
    url = f"{cfg['api']['base_url']}/Patient/{patient_id}"
    t = cfg["api"]["timeout_seconds"]
    r = requests.get(url, headers=_headers(cfg), timeout=t)
    r.raise_for_status()
    res = r.json()
    mutate(res)
    res.pop("meta", None)
    r2 = requests.put(url, json=res, headers=_headers(cfg), timeout=t)
    r2.raise_for_status()
    return _summary(r2.json(), r2)