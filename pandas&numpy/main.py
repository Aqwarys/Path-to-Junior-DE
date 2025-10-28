import time

inputs = []
s = input("Enter the data to write in the file: ")
while s != "exit":
    inputs.append(s)
    s = input("Enter the data to write in the file: ")

for _ in range(3):
    print("Processing...")
    time.sleep(1)

with open('pandas&numpy/yourfile.txt', 'w', encoding='utf-8') as f:
    for i in inputs:
        f.write(f'{i}\n')

with open('pandas&numpy/yourfile.txt', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    for line in lines:
        print(line, end='')