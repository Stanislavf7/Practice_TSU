#!/usr/bin/python3
import sys

def main():
	_ = True
	isterm = sys.stdin.isatty()
	while _:
		try:
			if (isterm):
				print("Привет, как тебя зовут?")
			line = sys.stdin.readline()
			if not line:
				break
			#мы не знаем как пользователь вводит имена в файл
			datalist = line.split()
			for data in datalist:
				try:
					if not (all(char.isalpha() for char in data)):
						raise Exception(f"Ошибка: {data} - не является именем!")
					elif data[0].islower():
						raise Exception(f"Ошибка: {data} - имя должно начинаться с заглавной буквы!")
					else:
						print(f"{data}, приятно познакомиться!")

				except Exception as e:
					print (e, file=sys.stderr)

		except KeyboardInterrupt:
			print("\nПока!")
			_ = False
	return

if __name__ == "__main__":
	main()
