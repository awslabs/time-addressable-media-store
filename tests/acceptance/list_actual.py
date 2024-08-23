def parse_output(file):
    with open(file, "r", encoding="utf-8") as fr:
        data = fr.read().splitlines()
    tests = [
        f'{line.split(" ")[0].split("::")[-1]}\n'
        for line in data
        if line.startswith("tests/")
    ]
    tests.sort()
    return tests


if __name__ == "__main__":
    with open("pytest_actual_names.txt", "w", encoding="utf-8") as fw:
        fw.writelines(parse_output("output.txt"))
