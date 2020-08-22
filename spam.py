import httplib2
import os
import random
from apiclient import discovery
from google.oauth2 import service_account
from prefect import task, Flow
from prefect.environments.storage import Docker

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = "1ShLYjnxdtjIu_CHOBjQvAK9XT3ZYNa6pWrPGkygn35I"
RANGE_NAME = "s!A1:E"
spam_pics = [
    "https://www.google.com/imgres?imgurl=https%3A%2F%2Fthumbs-prod.si-cdn.com%2FH7aVwwcff-NyQDH0opXs7a6BDUg%3D%2F800x600%2Ffilters%3Ano_upscale()%2Fhttps%3A%2F%2Fpublic-media.si-cdn.com%2Ffiler%2Fa3%2Fa5%2Fa3a5e93c-0fd2-4ee7-b2ec-04616b1727d1%2Fkq4q5h7f-1498751693.jpg&imgrefurl=https%3A%2F%2Fwww.smithsonianmag.com%2Ffood%2Fhow-spam-went-canned-necessity-american-icon-180963916%2F&tbnid=sW5zU9VA_eHrnM&vet=12ahUKEwj7s9PAya3rAhXGGd8KHdhTAWUQMygCegUIARCHAg..i&docid=92eeDi1DXkobYM&w=800&h=600&q=spam&ved=2ahUKEwj7s9PAya3rAhXGGd8KHdhTAWUQMygCegUIARCHAg",
    "https://www.google.com/imgres?imgurl=https%3A%2F%2Ffood.fnr.sndimg.com%2Fcontent%2Fdam%2Fimages%2Ffood%2Ffullset%2F2020%2F04%2F22%2FGettyImages-537799082_s4x3.jpg.rend.hgtvcom.616.462.suffix%2F1587588300587.jpeg&imgrefurl=https%3A%2F%2Fwww.foodnetwork.com%2Ffn-dish%2Frecipes%2F2020%2F4%2Fways-to-use-spam&tbnid=XSVVWDcfB-E5gM&vet=12ahUKEwj7s9PAya3rAhXGGd8KHdhTAWUQMygIegUIARCUAg..i&docid=A9uyL1WPxDkhQM&w=616&h=462&q=spam&ved=2ahUKEwj7s9PAya3rAhXGGd8KHdhTAWUQMygIegUIARCUAg",
    "https://www.google.com/imgres?imgurl=https%3A%2F%2Fcdn.vox-cdn.com%2Fthumbor%2FUO1hhAGb7ea5G-MuC43l1Sxx9Rw%3D%2F0x0%3A2282x1712%2F1200x675%2Ffilters%3Afocal(0x0%3A2282x1712)%2Fcdn.vox-cdn.com%2Fuploads%2Fchorus_image%2Fimage%2F50821489%2Fspam-wall.0.0.jpg&imgrefurl=https%3A%2F%2Fwww.eater.com%2F2014%2F7%2F9%2F6191681%2Fa-brief-history-of-spam-an-american-meat-icon&tbnid=LSwDDC1Uv429-M&vet=12ahUKEwj7s9PAya3rAhXGGd8KHdhTAWUQMygDegUIARCJAg..i&docid=ITcUBJmJEfa8TM&w=1200&h=675&q=spam&ved=2ahUKEwj7s9PAya3rAhXGGd8KHdhTAWUQMygDegUIARCJAg",
    "https://www.google.com/imgres?imgurl=https%3A%2F%2Fcdn.abcotvs.com%2Fdip%2Fimages%2F5675709_localish-BSZ1253-SPAMFRIES-KGO-vid.jpg&imgrefurl=https%3A%2F%2Fabc7news.com%2Flocalish%2Fspam-fries-will-be-your-new-guilty-pleasure%2F5675710%2F&tbnid=kaeFJKgykEEt8M&vet=12ahUKEwj7s9PAya3rAhXGGd8KHdhTAWUQMyghegUIARDTAg..i&docid=dfhnsNEhBSzHRM&w=1920&h=1080&q=spam&ved=2ahUKEwj7s9PAya3rAhXGGd8KHdhTAWUQMyghegUIARDTAg",
    "https://www.google.com/imgres?imgurl=https%3A%2F%2Fabcnews4.com%2Fresources%2Fmedia%2F140f50d8-1b38-4d61-ba99-59ba642ed8dc-large16x9_PumpkinSpiceSPAMHormel.PNG%3F1565885322319&imgrefurl=http%3A%2F%2Fabcnews4.com%2Fnews%2Foffbeat%2Fpumpkin-spice-spam-08152019&tbnid=hH8Tnd6cIapp1M&vet=12ahUKEwj7s9PAya3rAhXGGd8KHdhTAWUQMygwegUIARD0Ag..i&docid=9p5IFAsbdNLAQM&w=986&h=555&q=spam&ved=2ahUKEwj7s9PAya3rAhXGGd8KHdhTAWUQMygwegUIARD0Ag",
    "https://www.google.com/imgres?imgurl=https%3A%2F%2Fwww.adn.com%2Fresizer%2Fze5GgqQQs6L-FI-FxBs9yPblo4Y%3D%2F1200x0%2Fs3.amazonaws.com%2Farc-wordpress-client-uploads%2Fadn%2Fwp-content%2Fuploads%2F2018%2F09%2F27025144%2FIMG_6950.jpg&imgrefurl=https%3A%2F%2Fwww.adn.com%2Falaska-life%2Ffood-drink%2F2018%2F09%2F28%2Falaskana-recipe-spam-musubi-and-the-musubi-mcmuffin%2F&tbnid=ha6VFvQ1DEjpxM&vet=10CEcQMyiGAWoXChMI0KGVwsmt6wIVAAAAAB0AAAAAEAc..i&docid=oWUeUNsCvh3bKM&w=1200&h=996&q=spam&ved=0CEcQMyiGAWoXChMI0KGVwsmt6wIVAAAAAB0AAAAAEAc",
    "https://www.google.com/imgres?imgurl=https%3A%2F%2Fi1.wp.com%2Fwww.tastyislandhawaii.com%2Fimages14%2Fspam%2Fspam_teriyaki_musubi.jpg&imgrefurl=http%3A%2F%2Ftastyislandhawaii.com%2F2014%2F05%2F03%2Fproduct-review-spam-teriyaki%2F&tbnid=nKaxh1OJkRjusM&vet=10CFIQMyiLAWoXChMI0KGVwsmt6wIVAAAAAB0AAAAAEAc..i&docid=IAN9rlvU5PPjiM&w=1750&h=1840&q=spam&ved=0CFIQMyiLAWoXChMI0KGVwsmt6wIVAAAAAB0AAAAAEAc",
    "https://www.google.com/imgres?imgurl=https%3A%2F%2Fwww.howtogeek.com%2Fthumbcache%2F2%2F200%2F345ad41636294cabddb068d1b0c81aa4%2Fwp-content%2Fuploads%2F2019%2F01%2Fdrive-spam.png&imgrefurl=https%3A%2F%2Fwww.howtogeek.com%2F400511%2Fgoogle-drive-has-a-serious-spam-problem-and-google-doesnt-seem-to-care%2F&tbnid=eCxRlybS1ivYQM&vet=10CJYBEDMoqgFqFwoTCNChlcLJresCFQAAAAAdAAAAABAH..i&docid=m9rs2uM5hIARuM&w=1300&h=600&q=spam&ved=0CJYBEDMoqgFqFwoTCNChlcLJresCFQAAAAAdAAAAABAH",
    "https://www.google.com/imgres?imgurl=http%3A%2F%2Fimages.summitmedia-digital.com%2Fyummyph%2Fimages%2F2020%2F03%2F04%2Fpizza-hut-cheesy-spam-bites-pizza.jpg&imgrefurl=https%3A%2F%2Fwww.yummy.ph%2Fnews-trends%2Fpizza-hut-spam-cheesy-bites-pizza-a00260-20200305&tbnid=WsgOKhPQLolouM&vet=10CKkBEDMosAFqFwoTCNChlcLJresCFQAAAAAdAAAAABAH..i&docid=nZJuSDQw0dEXOM&w=640&h=360&q=spam&ved=0CKkBEDMosAFqFwoTCNChlcLJresCFQAAAAAdAAAAABAH",
    "https://www.google.com/imgres?imgurl=https%3A%2F%2Fwww.kroger.com%2Fproduct%2Fimages%2Flarge%2Fback%2F0003760013872&imgrefurl=https%3A%2F%2Fwww.kroger.com%2Fp%2Fspam-classic%2F0003760013872&tbnid=q9C5oQBmtImnnM&vet=10CMABEDMougFqFwoTCNChlcLJresCFQAAAAAdAAAAABAH..i&docid=jn4VkYxswWfX5M&w=500&h=500&q=spam&ved=0CMABEDMougFqFwoTCNChlcLJresCFQAAAAAdAAAAABAH",
    "https://www.google.com/imgres?imgurl=http%3A%2F%2Fwww.spamcanada.com%2Fwp-content%2Fuploads%2FInstant-noodle.jpg&imgrefurl=http%3A%2F%2Fwww.spamcanada.com%2Fen%2Frecipes%2Fspam-instant-noodles%2F&tbnid=1290TXyIOI_mJM&vet=10COsBEDMoywFqFwoTCNChlcLJresCFQAAAAAdAAAAABAH..i&docid=XMo2jHRXZP177M&w=900&h=600&q=spam&ved=0COsBEDMoywFqFwoTCNChlcLJresCFQAAAAAdAAAAABAH",
]


@task(name="Choose Spam Picture")
def choose_spam_picture():
    return random.choice(spam_pics)


@task(name="Insert Picture Into Google Sheet")
def insert_picture_into_google_sheet(spam_picture):
    # # The A1 notation of a range to search for a logical table of data.
    # # Values will be appended after the last row of the table.
    range_ = "A1:E"
    # How the input data should be interpreted.
    value_input_option = "USER_ENTERED"
    # How the input data should be inserted.
    value_range_body = {"values": ["spam"]}
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    secret_file = os.path.join(os.getcwd(), "creds.json")
    spreadsheet_id = "1EC_FIA2ZpydWxBsuUaQIUUpcYk3mm6VYWMs4Q4cn26c"
    range_name = "!A1:D2"
    credentials = service_account.Credentials.from_service_account_file(
        secret_file, scopes=scopes
    )
    service = discovery.build("sheets", "v4", credentials=credentials)
    values = [
        [spam_picture, "Powered by Prefect!", "Muahaha"],
    ]
    data = {"values": values}
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        body=data,
        range=range_name,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
    ).execute()


storage = Docker(
    prefect_version="0.13.3",
    registry_url="gcr.io/prefect-staging-5cd57f/data-warehouse/elt-to-data-warehouse/",
    base_image="python:3.8",
    python_dependencies=[
        "gcsfs",
        "google-cloud-firestore",
        "google-cloud-bigquery",
        "google-cloud-storage",
        "pandas",
        "pendulum",
        "psycopg2",
        "sqlalchemy",
    ],
    files={
        "/Users/dylanhughes/dev/service-account-keys/prefect-data-warehouse-4579f7d4de1d.json": "/root/.prefect/prefect-data-warehouse-credentials.json"
    },
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": "/root/.prefect/prefect-data-warehouse-credentials.json"
    },
)
with Flow("Spam Mikey!") as flow:
    spam_pic = choose_spam_picture()
    insert_picture_into_google_sheet(spam_pic)
