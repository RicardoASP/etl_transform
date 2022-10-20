from main_utils import Files

files = Files()

def test_read_file():
    payload = files.read_file(files.test_files,'test_data_file.txt',skip_header=False)
    assert payload == ["Diabetes  1  1\n", "Asthma    0-14\n","Stroke    1122"]

test_read_file()