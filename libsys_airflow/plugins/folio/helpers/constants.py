from numpy import nan

"""
  includes the default call number type Ids
"""
call_number_codes = {
    "0": "6caca63e-5651-4db6-9247-3205156e9699",
    "ALPHANUM": "28927d76-e097-4f63-8510-e56f2b7a3ad0",
    "ASIS": "28927d76-e097-4f63-8510-e56f2b7a3ad0",
    "DEWEY": "03dd64d0-5626-4ecd-8ece-4531e0069f35",
    "DEWEYPER": "03dd64d0-5626-4ecd-8ece-4531e0069f35",
    "LC": "95467209-6d7b-468b-94df-0f5d7ad2747d",
    "LCPER": "95467209-6d7b-468b-94df-0f5d7ad2747d",
    "SUDOC": "fc388041-6cd0-4806-8a74-ebe3b9ab4c6e",
}

see_other_lib_locs = {
    "ART": "ART-SEE-OTHER",
    "BUSINESS": "BUS-SEE-OTHER",
    "CLASSICS": "CLA-SEE-OTHER",
    "EARTH-SCI": "EAR-SEE-OTHER",
    "EAST-ASIA": "EAL-SEE-OTHER",
    "EDUCATION": "EDU-SEE-OTHER",
    "ENG": "ENG-SEE-OTHER",
    "GREEN": "GRE-SEE-OTHER",
    "HOOVER": "HILA-SEE-OTHER",
    "HOPKINS": "MAR-SEE-OTHER",
    "HV-ARCHIVE": "HILA-SEE-OTHER",
    "LAW": "LAW-SEE-OTHER",
    "MEDIA-MTXT": "MEDIA-SEE-OTHER",
    "MUSIC": "MUS-SEE-OTHER",
    "SAL": "SAL-SEE-OTHER",
    "SAL3": "SAL3-SEE-OTHER",
    "SCIENCE": "SCI-SEE-OTHER",
    "SPEC-COLL": "SPEC-SEE-OTHER",
    "SUL": "SUL-SEE-OTHER",
    "TANNER": "TAN-SEE-OTHER",
    "ARS": "ARS-SEE-OTHER",
    "RUMSEYMAP": "RUM-SEE-OTHER",
}

expense_codes = [
    {
        'acquisition method': 'Purchase',
        'order format': 'Electronic Resource',
        'material type': 'database',
        'Expense code': '53258',
    },
    {
        'acquisition method': 'Subscription',
        'order format': 'Electronic Resource',
        'material type': 'database',
        'Expense code': '53263',
    },
    {
        'acquisition method': 'Approval Plan',
        'order format': 'Electronic Resource',
        'material type': 'book',
        'Expense code': '53256',
    },
    {
        'acquisition method': 'Purchase',
        'order format': 'Electronic Resource',
        'material type': 'book',
        'Expense code': '53256',
    },
    {
        'acquisition method': 'Subscription',
        'order format': 'Electronic Resource',
        'material type': 'book',
        'Expense code': '53261',
    },
    {
        'acquisition method': 'Purchase',
        'order format': 'Electronic Resource',
        'material type': 'periodical',
        'Expense code': '53257',
    },
    {
        'acquisition method': 'Subscription',
        'order format': 'Electronic Resource',
        'material type': 'periodical',
        'Expense code': '53262',
    },
    {
        'acquisition method': 'Maintenance fee',
        'order format': 'Electronic Resource',
        'material type': nan,
        'Expense code': '53270',
    },
    {
        'acquisition method': nan,
        'order format': 'Physical Resource',
        'material type': 'sound recording',
        'Expense code': '53270',
    },
    {
        'acquisition method': nan,
        'order format': 'Physical Resource',
        'material type': 'map',
        'Expense code': '53270',
    },
    {
        'acquisition method': nan,
        'order format': 'Physical Resource',
        'material type': 'microform',
        'Expense code': '53250',
    },
    {
        'acquisition method': nan,
        'order format': 'Physical Resource',
        'material type': 'score',
        'Expense code': '53270',
    },
    {
        'acquisition method': nan,
        'order format': 'Physical Resource',
        'material type': 'software',
        'Expense code': '55410',
    },
    {
        'acquisition method': nan,
        'order format': 'Physical Resource',
        'material type': 'video recording',
        'Expense code': '53270',
    },
    {
        'acquisition method': 'Shipping',
        'order format': nan,
        'material type': nan,
        'Expense code': '55320',
    },
    {
        'acquisition method': nan,
        'order format': nan,
        'material type': nan,
        'Expense code': '53245',
    },
]

symphony_holdings_types_map = {
    "EQUIP": "Equipment",
    "MARC": "Book",
    "MANUSCRPT": "Archival Collection",
    "MAP": "Map",
    "MRDF": "Computer File",
    "RECORDING": "Recording",
    "SCORE": "Score",
    "SERIAL": "Serial",
    "TECHRPTS": "Book",
    "VM": "Visual Material",
}
