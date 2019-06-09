
#include <stdio.h>

#include <iostream>
#include <functional>
#include <string>

int main(){
	std::ifstream f("");
	printf(f.bad());
}
