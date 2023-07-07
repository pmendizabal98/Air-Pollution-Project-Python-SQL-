try:
    with open('air-quality-data-2003-2022.csv', 'r', encoding='utf-8') as infile, \
         open('crop.csv', 'w', encoding='utf-8') as outfile:
        
        # Write the header row to the output file
        header = infile.readline().strip()
        outfile.write(header + '\n')
        
        # Set the cutoff date as a string in the format 'YYYY-MM-DD'
        cutoff_date = '2010-01-01'
        
        # Iterate over each row in the input file
        for line in infile:
            # Extract the date and value fields
            date_str, value = line.strip().split(';')[:2]
            
            # Check if the date is on or after '2010-01-01'
            if date_str >= cutoff_date:
                # Write the row to the output file
                outfile.write(line)
        
except Exception as e:
    print(f"An error occurred: {e}")


