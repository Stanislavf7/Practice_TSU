#!/usr/bin/python3

import random
import sys

def main():
	try:
		data = input()
		A = int(data)
		B = random.randint(-10,10)
		res = A/B
	except ValueError:
	    print('Введены неверные данные', file=sys.stderr)
	except EOFError:
	    print('Данные не поступили', file=sys.stderr)
	except ZeroDivisionError:
		print('Деление на 0', file=sys.stderr)
	else:
		print(res)
	return

if __name__ == "__main__":
	main()

