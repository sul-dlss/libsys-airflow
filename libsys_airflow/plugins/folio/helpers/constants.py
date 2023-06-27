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

expense_codes = {
    '53245': {'acquisition_method': 'Default'},
    '53250': {
        'acquisition_method': 'Maintenance fee',
        'material_type': 'microform',
        'order_format': 'Physical Resource',
    },
    '53256': {
        'acquisition_method': 'Purchase',
        'material_type': 'book',
        'order_format': 'Electronic Resource',
    },
    '53257': {
        'acquisition_method': 'Purchase',
        'material_type': 'periodical',
        'order_format': 'Electronic Resource',
    },
    '53258': {
        'acquisition_method': 'Purchase',
        'material_type': 'database',
        'order_format': 'Electronic Resource',
    },
    '53261': {
        'acquisition_method': 'Subscription',
        'material_type': 'book',
        'order_format': 'Electronic Resource',
    },
    '53262': {
        'acquisition_method': 'Subscription',
        'material_type': 'periodical',
        'order_format': 'Electronic Resource',
    },
    '53263': {
        'acquisition_method': 'Subscription',
        'material_type': 'database',
        'order_format': 'Electronic Resource',
    },
    '53270': {
        'acquisition_method': 'Maintenance fee',
        'material_type': 'video recording',
        'order_format': 'Physical Resource',
    },
    '55320': {'acquisition_method': 'Shipping'},
    '55410': {
        'acquisition_method': 'Maintenance fee',
        'material_type': 'software',
        'order_format': 'Physical Resource',
    },
}
