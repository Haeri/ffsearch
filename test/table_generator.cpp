#include <iostream>
#include <fstream>
#include <sstream>
#include <random>
#include <string>
#include <vector>

constexpr size_t TOTAL_ROWS = 2400000;
constexpr size_t TOTAL_CLUSTERS = 900000;

std::vector<std::string> readNamesFromFile(const std::string &filename)
{
  std::ifstream file(filename);
  std::vector<std::string> names;
  std::string line;
  while (std::getline(file, line))
  {
    names.push_back(line);
  }
  return names;
}

int main()
{
  std::vector<std::string> firstNames = readNamesFromFile("./data/first_names.txt");
  std::vector<std::string> lastNames = readNamesFromFile("./data/last_names.txt");
  std::vector<std::string> countries = readNamesFromFile("./data/countries.txt");

  std::ofstream MyFile("names.csv");
  std::stringstream tmp;
  tmp << "id,full_name,cluster_id,nationality,country_of_residence\n";

  std::default_random_engine generator(42);
  std::uniform_int_distribution<size_t> distribution(0, TOTAL_CLUSTERS);

  for (size_t i = 0; i < TOTAL_ROWS; ++i)
  {
    size_t cluster_id = distribution(generator);

    tmp << i << "," << firstNames[rand() % firstNames.size()] << (rand() % 100 == 0 ? " " + firstNames[rand() % firstNames.size()] : "") << " " << lastNames[rand() % lastNames.size()] << ",id-" << cluster_id << "," << countries[rand() % countries.size()] << "," << countries[rand() % countries.size()] << "\n";

    if (i % 10000 == 0)
    {
      std::cout << i << "\n";
      MyFile << tmp.str();
      tmp.str("");
    }
  }

  MyFile << tmp.str();
  MyFile.close();

  std::cout << "Done\n";

  return 0;
}
