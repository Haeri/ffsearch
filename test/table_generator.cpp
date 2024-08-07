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
  std::uniform_int_distribution<size_t> cluster_distribution(0, TOTAL_CLUSTERS);
  std::uniform_int_distribution<size_t> first_name_distribution(0, firstNames.size() - 1);
  std::uniform_int_distribution<size_t> last_name_distribution(0, lastNames.size() - 1);
  std::uniform_int_distribution<size_t> countries_distribution(0, countries.size() - 1);
  std::uniform_int_distribution<int> hundred_distribution(0, 99);

  for (size_t i = 0; i < TOTAL_ROWS; ++i)
  {
    size_t cluster_id = cluster_distribution(generator);
    std::string first_name = firstNames[first_name_distribution(generator)];
    if (hundred_distribution(generator) == 0)
    {
      first_name += " " + firstNames[first_name_distribution(generator)];
    }
    std::string last_name = lastNames[last_name_distribution(generator)];
    std::string nationality = countries[countries_distribution(generator)];
    std::string country_of_residence = countries[countries_distribution(generator)];

    tmp << i << "," << first_name << " " << last_name << ",id-" << cluster_id << "," << nationality << "," << country_of_residence << "\n";

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
