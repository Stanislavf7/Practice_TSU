#!/usr/bin/python3

import sys

def main():
	try:
		data = input()
		C = float(data) 
		#зависит от постановки задачи, будем считать что ожидается положительное число
		if C<0:
			raise Exception('Полученное число - отрицательное!')
	except EOFError:
	    print('Данные не поступили', file=sys.stderr)
	except ValueError:
	    print('Введены неверные данные', file=sys.stderr)
	except Exception as e:
		print(e, file=sys.stderr)
	else:
		print(C**0.5)
	return

if __name__ == "__main__":
	main()  
