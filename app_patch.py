def parse_data(data):
    if not isinstance(data, list):
        raise ValueError('Input must be a list')
    parsed_data = []
    for index in range(len(data)):
        try:
            parsed_data.append(data[index])
        except IndexError:
            print(f'Index {index} is out of range.')
    return parsed_data