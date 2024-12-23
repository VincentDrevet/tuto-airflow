
select 
    "id" AS "CODE_STATION",
    "latitude" / 100000 AS "LATITUDE",
    "longitude" / 100000 AS "LONGITUDE",
    "Code postal" AS "ZIP_CODE",
    "Adresse" AS "ADDRESS",
    UPPER("Ville") AS "CITY",
    "Prix Gazole mis à jour le" AS "GAZOLE_UPDATE_DATE_PRICE",
    "Prix Gazole" AS "GAZOLE_PRICE",
    "Prix SP95 mis à jour le" AS "SP95_UPDATE_DATE_PRICE",
    "Prix SP95" AS "SP95_PRICE",
    "Prix E85 mis à jour le" AS "E85_UPDATE_DATE_PRICE",
    "Prix E85" AS "E85_PRICE",
    "Prix GPLc mis à jour le" AS "GPLC_UPDATE_DATE_PRICE",
    "Prix GPLc" AS "GPLC_PRICE",
    "Prix E10 mis à jour le" AS "E10_UPDATE_DATE_PRICE",
    "Prix E10" AS "E10_PRICE",
    "Prix SP98 mis à jour le" "SP98_UPDATE_DATE_PRICE",
    "Prix SP98" AS "SP98_PRICE",
    "code_departement" AS "CODE_DEPARTEMENT",
    "code_region" AS "CODE_REGION",
    "DT_TRANSACTION"
FROM raw_data