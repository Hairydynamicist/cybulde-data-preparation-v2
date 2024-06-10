from google.cloud import secretmanager


def access_secret_version(project_id: str, secret_id: str, version_id: str = "1") -> str:
    """
    access the payload for a given secrete version if one exists,
    The cersion can be a version number as string (e.g. "S") or an alias (e.g. "latest")
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("UTF-8")
    return payload
