#include <iostream>
#include <string>

int main(){
    std::string name;
    std::cout << "Please enter your first name: ";
    std::cin >> name;
    std::cout << "Welcome " << name << "!" << std::endl;

    const char* name2 = "Ohm";
    char letters[3] = {'O', 'h', 'm'};
    std::cout << letters << std::endl;
    std::cin.get();

    return 0;
}