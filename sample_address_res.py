def address_parser(root, ns, country_mapping):
    """Parses addresses from the XML root and returns field names and data rows."""
    fieldnames = [
        "ID",
        "FixedRef",
        "AreaCodeID",
        "Country",
        "CountryRelevanceID",
        "FeatureVersionID",
        "Unknown",
        "Region",
        "Address 1",
        "Address 2",
        "Address 3",
        "City",
        "State/ Province",
        "Postal Code",
        "Script Type",
    ]
    data_rows = []

    # Create a mapping from IdentityID to FixedRef
    identity_to_fixed_ref = {}
    for party in root.findall(".//ns:DistinctParty", ns):
        fixed_ref = party.attrib["FixedRef"]
        for profile in party.findall(".//ns:Profile", ns):
            for identity in profile.findall(".//ns:Identity", ns):
                identity_id = identity.attrib["ID"]
                identity_to_fixed_ref[identity_id] = fixed_ref

    # Track the first occurrence of each ID to set the Script Type to "Latin"
    first_occurrence = set()

    # Process each Location and write data to CSV
    locations = root.findall(".//ns:Location", ns)
    for location in locations:
        location_id = location.attrib["ID"]
        area_code_id = location.find(".//ns:LocationAreaCode", ns)
        area_code_id = (
            area_code_id.attrib["AreaCodeID"] if area_code_id is not None else ""
        )

        country = location.find(".//ns:LocationCountry", ns)
        country_id = country.attrib["CountryID"] if country is not None else ""

        # Added condition to set country to "undetermined" for area code 11291
        if area_code_id == "11291" and not country_id:
            country_name = "undetermined"
        else:
            country_name = country_mapping.get(country_id, "")

        # Retrieve FixedRef using IdentityID
        id_reg_document_ref = location.find(".//ns:IDRegDocumentReference", ns)
        if id_reg_document_ref is not None:
            identity_id = id_reg_document_ref.attrib["IDRegDocumentID"]
            fixed_ref = identity_to_fixed_ref.get(identity_id, "")
        else:
            fixed_ref = ""

        # Initialize data dictionary
        data = {
            "ID": location_id,
            "FixedRef": fixed_ref,
            "AreaCodeID": area_code_id,
            "Country": country_name,
            "FeatureVersionID": "",
            "Unknown": "",
            "Region": "",
            "Address 1": "",
            "Address 2": "",
            "Address 3": "",
            "City": "",
            "State/ Province": "",
            "Postal Code": "",
            "Script Type": "",
        }

        # Collect non-Latin script values
        non_latin_data = {
            "Chinese Simplified": {
                "Unknown": "",
                "Region": "",
                "Address 1": "",
                "Address 2": "",
                "Address 3": "",
                "City": "",
                "State/ Province": "",
                "Postal Code": "",
            },
            "Chinese Traditional": {
                "Unknown": "",
                "Region": "",
                "Address 1": "",
                "Address 2": "",
                "Address 3": "",
                "City": "",
                "State/ Province": "",
                "Postal Code": "",
            },
            "Cyrillic": {
                "Unknown": "",
                "Region": "",
                "Address 1": "",
                "Address 2": "",
                "Address 3": "",
                "City": "",
                "State/ Province": "",
                "Postal Code": "",
            },
            "Arabic": {
                "Unknown": "",
                "Region": "",
                "Address 1": "",
                "Address 2": "",
                "Address 3": "",
                "City": "",
                "State/ Province": "",
                "Postal Code": "",
            },
            "Japanese": {
                "Unknown": "",
                "Region": "",
                "Address 1": "",
                "Address 2": "",
                "Address 3": "",
                "City": "",
                "State/ Province": "",
                "Postal Code": "",
            },
        }

        for part in location.findall(".//ns:LocationPart", ns):
            part_type_id = part.attrib["LocPartTypeID"]
            for part_value in part.findall(".//ns:LocationPartValue", ns):
                value = (
                    part_value.find(".//ns:Value", ns).text
                    if part_value.find(".//ns:Value", ns) is not None
                    else ""
                )
                comment = (
                    part_value.find(".//ns:Comment", ns).text
                    if part_value.find(".//ns:Comment", ns) is not None
                    else ""
                )

                if not comment:
                    if part_type_id == "1":
                        data["Unknown"] = value
                    elif part_type_id == "1450":
                        data["Region"] = value
                    elif part_type_id == "1451":
                        data["Address 1"] = value
                    elif part_type_id == "1452":
                        data["Address 2"] = value
                    elif part_type_id == "1453":
                        data["Address 3"] = value
                    elif part_type_id == "1454":
                        data["City"] = value
                    elif part_type_id == "1455":
                        data["State/ Province"] = value
                    elif part_type_id == "1456":
                        data["Postal Code"] = value
                else:
                    if comment not in non_latin_data:
                        non_latin_data[comment] = {
                            "Unknown": "",
                            "Region": "",
                            "Address 1": "",
                            "Address 2": "",
                            "Address 3": "",
                            "City": "",
                            "State/ Province": "",
                            "Postal Code": "",
                        }

                    if part_type_id == "1":
                        non_latin_data[comment]["Unknown"] = value
                    elif part_type_id == "1450":
                        non_latin_data[comment]["Region"] = value
                    elif part_type_id == "1451":
                        non_latin_data[comment]["Address 1"] = value
                    elif part_type_id == "1452":
                        non_latin_data[comment]["Address 2"] = value
                    elif part_type_id == "1453":
                        non_latin_data[comment]["Address 3"] = value
                    elif part_type_id == "1454":
                        non_latin_data[comment]["City"] = value
                    elif part_type_id == "1455":
                        non_latin_data[comment]["State/ Province"] = value
                    elif part_type_id == "1456":
                        non_latin_data[comment]["Postal Code"] = value

        # Set Script Type to "Latin" for the first occurrence of each ID
        if data["ID"] not in first_occurrence:
            data["Script Type"] = "Latin"
            first_occurrence.add(data["ID"])

        # Add the Latin script values to data_rows
        data_rows.append(data)

        # Add the non-Latin script values to data_rows
        for script_type, values in non_latin_data.items():
            if (
                values["Unknown"]
                or values["Region"]
                or values["Address 1"]
                or values["Address 2"]
                or values["Address 3"]
                or values["City"]
                or values["State/ Province"]
                or values["Postal Code"]
            ):
                non_latin_row = data.copy()
                non_latin_row["Unknown"] = values["Unknown"]
                non_latin_row["Region"] = values["Region"]
                non_latin_row["Address 1"] = values["Address 1"]
                non_latin_row["Address 2"] = values["Address 2"]
                non_latin_row["Address 3"] = values["Address 3"]
                non_latin_row["City"] = values["City"]
                non_latin_row["State/ Province"] = values["State/ Province"]
                non_latin_row["Postal Code"] = values["Postal Code"]
                non_latin_row["Script Type"] = script_type
                data_rows.append(non_latin_row)

    return fieldnames, data_rows
