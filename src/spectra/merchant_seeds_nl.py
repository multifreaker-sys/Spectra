"""Dutch / NL-focused merchant seed data for local categorization.

These seeds are based on common Dutch bank exports (including ING) and are
intended to complement the global merchant list in ``ml_classifier.py``.
"""

from __future__ import annotations

# Format: (examples, category)
SEED_MERCHANTS_NL: list[tuple[list[str], str]] = [
    # Income / transfers
    (["SIERS LEIDING MONTAGE PR"], "Salary"),
    ([
        "Oranje Spaarrekening",
        "Hr EEI Kamp",
        "E.E.I. Kamp",
        "EEI Kamp",
        "Herrn E. Kamp",
        "M.P.S. Kooij",
        "Isai",
        "AAB INZ TIKKIE",
        "Albert Rotterink",
        "International Card Services B.V.",
    ], "Transfer"),
    (["Sociale Verzekeringsbank"], "Other Income"),

    # Government / taxes
    ([
        "BELASTINGDIENST",
        "GBTwente",
        "GBLT",
        "Landkreis Grafschaft Bentheim",
        "SAMTGEMEINDE UELSEN",
        "BNG Gemeente Almelo NLD",
        "Gemeente Hengelo OV",
    ], "Taxes"),

    # Housing / debt
    ([
        "ING Hypotheken",
        "Vereniging van eigenaars Werfstraat 1-1 tot en met 1-16.",
        "Bisschopstraat 39 Weerselo NLD",
    ], "Housing"),
    (["DEFAM", "Klarna Bank AB"], "Debt Repayment"),

    # Insurance
    ([
        "VGZ ZORGVERZEKERAAR NV",
        "Nationale-Nederlanden",
        "NN Schadeverzekering Mij NV betr ING Verzekeren",
        "VOOGD VOOGD VERZEKERING",
        "Monuta Verzekeringen NV",
        "Monuta Verzekeringen N.V.",
        "Unive Oost",
        "Unive Oost Bemiddeling B.V.",
    ], "Insurance"),

    # Utilities & telecom
    ([
        "VITENS NV",
        "DELTA ENERGIE B.V.",
        "DELTA Energie B.V.",
        "E.ON Energie Deutschland GmbH",
        "Coolblue Energie",
        "KPN B.V.",
        "KPN - Mobiel",
        "Grafschafter Breitband",
    ], "Utilities"),
    (["Kosten BetaalPakket"], "Bank Fees"),

    # Groceries
    ([
        "PLUS Schuldink ALMELO NLD",
        "Plus 212 VASSE NLD",
        "PLUS Pleijhuis B.V. TUBBERGEN",
        "PLUS Vriezenveen VRIEZENVEEN NLD",
        "Plus van Limbeek s9 ALMELO NLD",
        "Plus Wallerbosch GEESTEREN OV",
        "AH 8571 OOTMARSUM NLD",
        "AH Westerik Almelo ALMELO NLD",
        "BCK*AH 8565 HARDENBERG NLD",
        "Albert Heijn Almelo NLD",
        "Jumbo Hardenberg Adm HARDENBERG",
        "WASDAS HARDENBERG B.V. NLD",
        "Welkoop Ootmarsum OOTMARSUM NLD",
        "WELKOOP ALMELO ALMELO NLD",
        "Theijink Kaas HARDENBERG NLD",
    ], "Groceries"),

    # Transport
    ([
        "FREIE TANKST. VORRINK Itterb DEU",
        "FREIE TANKST. VORRINK ITTERB DEU",
        "RAIFFEISEN EMS VECHTE WILSUM DEU",
        "RAIFFEISEN EMS VECHTE UELSEN DEU",
        "RAIFFEISENWAREN ITTERB ITTER DEU",
        "VB NIEDERGRAFSCHAFT EG ITTER DEU",
        "TINQ OLDENZAAL EEKBOER OLDENZAAL",
        "Tango Voorst VOORST GEM VO NLD",
        "YELLOWBRICK BY BUCKAROO",
        "COMBI 059 Uelsen DEU",
        "ANWB Almelo ALMELO NLD",
    ], "Transport"),

    # Health
    ([
        "Etos 7905 ALMELO NLD",
        "Etos B.V.",
        "Kruidvat 4936 BORNE NLD",
        "Kruidvat 7468 ALMELO NLD",
        "Kruidvat Hardenberg NLD",
        "Drogisterij Koopmans OOTMARSUM",
    ], "Health"),

    # Shopping
    ([
        "BOLCOM BV",
        "BOL.COM",
        "ABOUT YOU",
        "Crocs.eu",
        "OTTO Payments GmbH",
        "OTTO Payments",
        "Action 1098 Almelo NLD",
        "Hema Hardenberg HARDENBERG NLD",
        "HEMA EV268 BORNE BORNE NLD",
        "Plein.NL",
        "Texelse Producten",
        "Baby Natura",
        "Bruna Hardenberg NLD",
        "Primera van Limbeek ALMELO NLD",
    ], "Shopping"),

    # Food & dining
    ([
        "Iberico Fino via Stripe Technology Europe Ltd",
        "De Lief-Hebbers via Stichting Mollie Payments",
        "Reiger en de Raaf via Stichting Mollie Payments",
        "CCV*Beerlage IJssalon ALMELO NLD",
        "BCK*IJscotheek Van Olf ALMELO",
        "BCK Danos Kip Grill HENGELO OV",
        "CCV*P. Hoang HOOGEVEEN NLD",
        "CCV*ReinerinksVis GEESTEREN OV",
        "Xpress Ootmarsum OOTMARSUM NLD",
        "XPRESS OOTMARSUM OOTMARSUM NLD",
    ], "Food & Dining"),

    # Entertainment
    (["Monkey Town Hardenberg NLD", "AVONTURENPARK HELLENDO NLD", "Musketiers"], "Entertainment"),
]

