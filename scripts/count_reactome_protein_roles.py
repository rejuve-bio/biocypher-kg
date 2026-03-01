
def extract_protein_roles_from_file(filename: str) -> tuple[int, list[str]]:
    """
    Extracts all unique protein roles from a Reactome file specified by its filename.

    Args:
        filename (str): The path to the Reactome file.
                        Each line in the file should have tab-separated fields,
                        and the last field should contain roles in the format [role1, role2].

    Returns:
        tuple[int, list[str]]: A tuple containing the number of unique roles
                                and an alphabetically sorted list of these roles.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        IOError: For other issues encountered while reading the file.
    """
    unique_roles = set() # We use a set to automatically store unique roles
    
    try:
        with open(filename, 'r', encoding='utf-8') as f: # Open the file for reading
            for line in f: # Iterate directly over the lines of the file
                line = line.strip() # Remove whitespace and newlines
                if not line:
                    continue # Skip empty lines
                    
                # Split the line by tabs to get the columns
                columns = line.split('\t')
                
                # Check for minimum number of columns expected (e.g., at least two for roles to be last)
                # In your sample, there are at least 5 columns, with role in the last one.
                # So, if len(columns) < 5 (or less conservatively, < 2), it's malformed for this context.
                if len(columns) < 2: 
                    # Optionally, you might want to log this or handle it differently
                    # For now, we skip it as it likely doesn't contain role info.
                    continue 
                    
                # The last column contains the roles (e.g., "[input, output, catalyst]")
                roles_string = columns[-1]
                
                # Remove brackets and split the string by commas
                # E.g., "[input, output]" -> "input, output" -> ["input", " output"]
                clean_roles_string = roles_string.replace('[', '').replace(']', '')
                
                # Split by commas and strip whitespace for each individual role
                individual_roles = [role.strip() for role in clean_roles_string.split(',')]
                
                # Add each cleaned role to the set of unique roles
                for role in individual_roles:
                    if role: # Ensures we don't add empty strings resulting from "[]" or ",,"
                        unique_roles.add(role)
                        
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found. Please check the path and filename.")
        return 0, [] # Return 0 unique roles and an empty list upon error
    except Exception as e:
        print(f"An unexpected error occurred while reading the file: {e}")
        return 0, [] # Handle other potential IO errors

    # Convert the set to a list and sort it for consistent output
    sorted_unique_roles = sorted(list(unique_roles))
    
    return len(sorted_unique_roles), sorted_unique_roles

if __name__ == "__main__":
    filename = "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/full/reactome/reactome_reaction_exporter_All_species.txt"
    num_roles, roles = extract_protein_roles_from_file(filename)
    print(f"Number of unique protein roles: {num_roles}")
    print(f"Unique protein roles: {roles}")