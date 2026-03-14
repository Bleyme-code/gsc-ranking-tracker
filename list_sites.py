"""Liste tous les sites accessibles dans votre compte Google Search Console."""
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
creds = Credentials.from_authorized_user_file("token.json", SCOPES)
service = build("searchconsole", "v1", credentials=creds)

response = service.sites().list().execute()
sites = response.get("siteEntry", [])

if not sites:
    print("Aucun site trouvé. Vérifiez que ce compte Google a accès à des propriétés GSC.")
else:
    print(f"\n{'='*60}")
    print(f"  {len(sites)} site(s) trouvé(s) dans votre compte GSC :")
    print(f"{'='*60}\n")
    for site in sites:
        print(f"  URL : {site['siteUrl']}")
        print(f"  Niveau : {site['permissionLevel']}")
        print()

print("\nCopiez les URLs exactes ci-dessus dans votre config.yaml")
