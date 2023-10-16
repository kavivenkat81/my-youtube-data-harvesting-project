data_youtube_list = []

# Outer loop
for i in range(3):
    # Initialize a dictionary for each iteration
    data_youtube1 = {}

    # Inner loop
    for j in range(2):
        key = f'key_{i}_{j}'
        value = i * j
        # Update the data_youtube1 dictionary with key-value pairs from the inner loop
        data_youtube1[key] = value

    # Append the dictionary to the list for this iteration
    data_youtube_list.append(data_youtube1)

# Now, data_youtube_list contains dictionaries from all iterations of the outer loop
for data_youtube1 in data_youtube_list:
    print(data_youtube1)
