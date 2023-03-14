#include <iostream>
#include <unordered_map>
#include <vector>
#include <set>
#include <string>
#include <string.h>
#include <fstream>
#include <algorithm>
#include <sstream>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string_view>

#ifdef _MSC_VER
#include <Windows.h>
#else
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#endif

#define PAGE_SIZE 1000000


using namespace std::chrono;


const std::string PAGE_PREFIX = "page_";
const std::string TABLE_DIR = "tables";

const char SINGLE_WILDCARD = '?';
const char MULTI_WILDCARD = '*';

struct TreeNode
{
	std::unordered_map <unsigned char, TreeNode*> children;
	std::vector<int> index;
};


TreeNode* trie_cache;

#ifdef _MSC_VER
struct FileContents {
	size_t size;
	char* buffer;

	LPVOID _mapped;
	HANDLE _hMapping;
	HANDLE _hFile;
};
FileContents fast_read(const std::string& file_path) {
	std::ifstream file(file_path);
	if (!file) {
		std::cerr << "Error: Failed to open file." << std::endl;
		return {};
	}

	FileContents fc{};

	// Open the file for reading and mapping into memory
	fc._hFile = CreateFile(file_path.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	if (fc._hFile == INVALID_HANDLE_VALUE) {
		std::cerr << "Error: Failed to open file for memory mapping." << std::endl;
		return {};
	}
	fc._hMapping = CreateFileMapping(fc._hFile, NULL, PAGE_READONLY, 0, 0, NULL);
	if (fc._hMapping == NULL) {
		std::cerr << "Error: Failed to map file into memory." << std::endl;
		CloseHandle(fc._hFile);
		return {};
	}
	fc._mapped = MapViewOfFile(fc._hMapping, FILE_MAP_READ, 0, 0, 0);
	if (fc._mapped == NULL) {
		std::cerr << "Error: Failed to map file into memory." << std::endl;
		CloseHandle(fc._hMapping);
		CloseHandle(fc._hFile);
		return {};
	}



	fc.size = GetFileSize(fc._hFile, NULL);
	fc.buffer = static_cast<char*>(fc._mapped);


	return fc;
}
void close_fast_file(FileContents fc) {
	UnmapViewOfFile(fc._mapped);
	CloseHandle(fc._hMapping);
	CloseHandle(fc._hFile);
}

#else
struct FileContents {
	size_t size;
	char* buffer;

	void* _mapped;
};

FileContents fast_read(const std::string& file_path) {
	int fd = open(file_path.c_str(), O_RDONLY);
	if (fd == -1) {
		std::cerr << "Error: Failed to open file." << std::endl;
		return {};
	}

	struct stat st;
	if (fstat(fd, &st) == -1) {
		std::cerr << "Error: Failed to get file size." << std::endl;
		close(fd);
		return {};
	}

	FileContents fc{};
	fc.size = static_cast<size_t>(st.st_size);

	// Map the file into memory
	fc._mapped = mmap(nullptr, fc.size, PROT_READ, MAP_PRIVATE, fd, 0);
	if (fc._mapped == MAP_FAILED) {
		std::cerr << "Error: Failed to map file into memory." << std::endl;
		close(fd);
		return {};
	}
	fc.buffer = static_cast<char*>(fc._mapped);

	close(fd);
	return fc;
}

void close_fast_file(FileContents fc) {
	// Unmap the file from memory
	munmap(fc._mapped, fc.size);
}
#endif


std::vector<std::string> split(const std::string& input, const char& delimiter)
{
	std::vector<std::string> elements;
	elements.reserve(std::count(input.begin(), input.end(), delimiter) + 1); // reserve space for all expected elements
	std::stringstream stream(input);
	std::string element;

	while (getline(stream, element, delimiter)) {
		elements.push_back(std::move(element));
	}

	return elements;
}

std::string& padd(std::string& text, int size) {
	if (text.size() < size) {
		text.insert(text.end(), size - text.size(), ' ');
	}
	else {
		text.replace(size - 3, 3, "...");
		text = text.substr(0, size);
	}
	return text;
}

std::string to_lowercase(const std::string& s) {
	std::string result = s;
	std::transform(result.begin(), result.end(), result.begin(),
		[](unsigned char c) { return std::tolower(c); });
	return result;
}

void remove_diacritics(unsigned char* p) {
	//char* p = str;
	//while ((*p) != 0) {
		const char*
			//   "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ"
			tr = "AAAAAAECEEEEIIIIDNOOOOOx0UUUUYPsaaaaaaeceeeeiiiiOnooooo/0uuuuypy";
		unsigned char ch = (*p);
		if (ch >= 192) {
			(*p) = tr[ch - 192];
		}
		//++p; // http://stackoverflow.com/questions/14094621/
	//}
	//return str;
}


void serialize_node(TreeNode* node, std::ofstream& outfile)
{
	// write the number of children
	size_t num_children = node->children.size();
	outfile.write(reinterpret_cast<const char*>(&num_children), sizeof(num_children));

	// write the indices associated with this node
	size_t num_indices = node->index.size();
	outfile.write(reinterpret_cast<const char*>(&num_indices), sizeof(num_indices));
	for (int i : node->index) {
		outfile.write(reinterpret_cast<const char*>(&i), sizeof(i));
	}

	// recursively serialize each child node
	for (auto& entry : node->children) {
		outfile.write(reinterpret_cast<const char*>(&entry.first), sizeof(entry.first));
		serialize_node(entry.second, outfile);
	}
}

void serialize_trie(TreeNode* root, const std::string& filename)
{
	std::cout << "Create tree " << filename << std::endl;
	std::ofstream outfile(filename, std::ios::binary);
	if (!outfile.is_open()) {
		std::cerr << "Error: could not write to file " << filename << std::endl;
		return;
	}
	serialize_node(root, outfile);
}

void deserialize_node(TreeNode* node, std::ifstream& infile)
{
	// read the number of children
	size_t num_children;
	infile.read(reinterpret_cast<char*>(&num_children), sizeof(num_children));

	// read the indices associated with this node
	size_t num_indices;
	infile.read(reinterpret_cast<char*>(&num_indices), sizeof(num_indices));
	if (num_indices > 0) {
		node->index.resize(num_indices);
		infile.read(reinterpret_cast<char*>(&(node->index[0])), sizeof(int) * num_indices);
	}

	// recursively deserialize each child node
	for (size_t i = 0; i < num_children; i++) {
		unsigned char key;
		infile.read(reinterpret_cast<char*>(&key), sizeof(key));
		TreeNode* child = new TreeNode();
		node->children[key] = child;
		deserialize_node(child, infile);
	}
}

TreeNode* deserialize_trie(const std::string& filename)
{
	std::ifstream infile(filename, std::ios::binary);
	if (!infile.is_open()) {
		std::cerr << "Error: could not open file " << filename << std::endl;
		return nullptr;
	}
	TreeNode* root = new TreeNode();
	deserialize_node(root, infile);
	return root;
}



void insert_token(TreeNode* root, const std::string& token, int index)
{
	TreeNode* tnp = root;

	for (int i = 0; i < token.size(); ++i) {
		TreeNode* tn;
		unsigned char key = tolower(token[i]);		
		remove_diacritics(&key);

		if (tnp->children.find(key) == tnp->children.end()) {
			tn = new TreeNode();
		}
		else {
			tn = tnp->children[key];
		}

		tnp->children[key] = tn;
		tnp = tn;
	}
	tnp->index.push_back(index);
}

std::vector<int> find_token(TreeNode* root, const std::string& token)
{
	TreeNode* tnp = root;
	std::vector<int> ret;
	//std::set<int> ret;

	for (int i = 0; i < token.size(); ++i) {
		unsigned char key = tolower(token[i]);
		//std::cout << key << " -> ";
		remove_diacritics(&key);
		//std::cout << key << std::endl;

		if (key == SINGLE_WILDCARD) {
			for (auto const& child : tnp->children) {
				auto tmp = find_token(child.second, token.substr(i + 1));
				ret.insert(ret.end(), tmp.begin(), tmp.end());
				//ret.insert(tmp.begin(), tmp.end());
			}
			return std::vector(ret.begin(), ret.end());
		}
		else if (key == MULTI_WILDCARD) {
			for (auto const& child : tnp->children) {
				auto tmp = find_token(child.second, SINGLE_WILDCARD + token.substr(i + 1));
				ret.insert(ret.end(), tmp.begin(), tmp.end());
				//ret.insert(tmp.begin(), tmp.end());
			//}
			//for (auto const& child : tnp->children) {
				tmp = find_token(child.second, MULTI_WILDCARD + token.substr(i + 1));
				ret.insert(ret.end(), tmp.begin(), tmp.end());
				//ret.insert(tmp.begin(), tmp.end());
			}
			return std::vector(ret.begin(), ret.end());
		}
		else if (tnp->children.find(key) == tnp->children.end()) {
			return {};
		}
		else {
			tnp = tnp->children[key];
		}
	}

	ret.insert(ret.end(), tnp->index.begin(), tnp->index.end());
	//ret.insert(tnp->index.begin(), tnp->index.end());

	return ret;
	//return std::vector(ret.begin(), ret.end());
}



void generate_table_pages(const std::string& path, const std::vector<std::string>& table) {
	for (int i = 0; i < table.size() / PAGE_SIZE; ++i) {
		std::string tablePageFileName = path + "/" + PAGE_PREFIX + std::to_string(i);
		std::ofstream outfile(tablePageFileName, std::ios::binary);

		size_t max_row_size = 0;
		size_t row_count = 0;
		for (int j = i * PAGE_SIZE; j < (i + 1) * PAGE_SIZE && j < table.size(); ++j) {
			if (table[j].size() > max_row_size) max_row_size = table[j].size();
			++row_count;
		}
		max_row_size += 1; // \0

		char* chunks = new char[max_row_size * row_count];
		size_t offset = 0;
		for (int j = i * PAGE_SIZE; j < (i + 1) * PAGE_SIZE && j < table.size(); ++j) {
#ifdef _MSC_VER
			strncat_s(chunks + offset, max_row_size, table[j].c_str(), max_row_size);
#else
			strncat(chunks + offset, table[j].c_str(), max_row_size);
#endif
			offset += max_row_size;
		}

		outfile.write(reinterpret_cast<const char*>(&max_row_size), sizeof(max_row_size));
		outfile.write(reinterpret_cast<const char*>(&row_count), sizeof(row_count));
		outfile.write(reinterpret_cast<const char*>(chunks), sizeof(char) * (max_row_size)*row_count);

		outfile.close();
	}
}


struct SchemaColumnIndex {
	std::string column;
	int index;
	TreeNode* root;
};


bool ff_index(const std::string& fileName, const std::set<std::string>& columns) {
	std::cout << "Indexing started for " << fileName << std::endl;

	std::string tableName = split(fileName, '.')[0];
	std::string tablePath = TABLE_DIR + "/" + tableName;

	if (!std::filesystem::is_directory(tablePath) || !std::filesystem::exists(tablePath)) {
		std::filesystem::create_directories(tablePath);
	}

	std::vector<std::string> table;

	//TreeNode* root = new TreeNode();
	std::string line;
	std::ifstream scv_file(fileName);
	int line_index = 0;

	//std::unordered_map<int, TreeNode*> column_map;
	std::vector<SchemaColumnIndex> schema_column_index;

	if (scv_file.is_open()) {
		
		// Get schema
		getline(scv_file, line);
		std::vector<std::string> schema = split(line, ',');

		int schema_index = 0;
		for (auto const& column : schema) {
			if (columns.find(column) != columns.end()) {
				schema_column_index.push_back({
					column,
					schema_index,
					new TreeNode
					});
			}
			++schema_index;
		}


		while (getline(scv_file, line)) {

			table.push_back(line);

			for (auto const& index_column: schema_column_index) {
				std::vector<std::string> row_elements = split(line, ',');
				std::string element = to_lowercase(row_elements[index_column.index]);
				std::vector<std::string> element_parts = split(element, ' ');
				std::set<std::string> unique_element_parts(element_parts.begin(), element_parts.end());

				for (auto const& unique_element_part : unique_element_parts) {
					insert_token(index_column.root, unique_element_part, line_index);
				}
			}

			++line_index;
			if (line_index % 1000000 == 0) {
				std::cout << "Indexing " << line_index << std::endl;
				//break;
			}
		}
		scv_file.close();
	}

	for (auto const& index_column : schema_column_index) {
	//for (auto const& column : columns) {
		std::string dir_name = tablePath + "/index/" + index_column.column + "/trie/";
		if (!std::filesystem::is_directory(dir_name) || !std::filesystem::exists(dir_name)) {
			std::filesystem::create_directories(dir_name);
		}
		for (auto const& alphabetIndex : index_column.root->children) {
			serialize_trie(alphabetIndex.second, dir_name + std::to_string(alphabetIndex.first));
		}
	}

	generate_table_pages(tablePath, table);

	return true;
}


struct TablePage {
	size_t row_size;
	size_t num_rows;
	char* buffer;

	FileContents _fc;
};

std::unordered_map<int, TablePage> pageMap;

std::string read_cache(const std::string& table, int index) {
	int page = index / PAGE_SIZE;
	int line = index - (page * PAGE_SIZE);

	TablePage tp{};

	if (pageMap.find(page) == pageMap.end()) {
		auto start = high_resolution_clock::now();

		tp._fc = fast_read(TABLE_DIR + "/" + table + "/" + PAGE_PREFIX + std::to_string(page));

		tp.row_size = *((size_t*)tp._fc.buffer);
		tp.num_rows = *((size_t*)(tp._fc.buffer + sizeof(size_t)));
		tp.buffer = (tp._fc.buffer + 2 * sizeof(size_t));

		pageMap[page] = tp;
	}
	else {
		tp = pageMap[page];
	}

	return std::string(tp.buffer + (line * tp.row_size)); // , tp.row_size);
}


struct ScoredResult {
	int index;
	float score;
};



std::vector<ScoredResult> find_all_tokens(const std::string& input, const std::string& table, const std::string& column, bool fuzzy) {
	if (trie_cache == nullptr)
		trie_cache = new TreeNode;

	std::unordered_map<int, int> results;
	std::vector<std::string> tokens;

	if (fuzzy) {
		std::vector<std::string> _tokens = split(input, ' ');

		for (const std::string& token : _tokens) {
			tokens.push_back(token);
			for (int i = 0; i < token.size(); ++i) {
				tokens.push_back(token.substr(0, i) + "?" + token.substr(i)); // INSERTION
				tokens.push_back(token.substr(0, i) + token.substr(i + 1)); // DELETION
				tokens.push_back(token.substr(0, i) + "?" + token.substr(i + 1)); // SUBSTITUTION

				if (i < token.size() - 1) { // TRANSPOSITION
					std::string cpy(token);
					std::swap(cpy[i], cpy[i + 1]);
					tokens.push_back(cpy);
				}
			}
			tokens.push_back(token + "?");
		}

		/*for (const std::string& token : tokens) {
			std::cout << token << std::endl;
		}*/
	}
	else {
		tokens = split(input, ' ');
	}


	for (const std::string& token : tokens) {
		unsigned char key = tolower(token[0]);
		//std::cout << key << " -> ";
		remove_diacritics(&key);
		//std::cout << key << std::endl;

		TreeNode* tn;

		if (trie_cache->children.find(key) == trie_cache->children.end()) {
			tn = deserialize_trie(TABLE_DIR + "/" + table + "/index/" + column + "/trie/" + std::to_string(key));
			if (tn == nullptr) continue;
			trie_cache->children[key] = tn;
		}
		else {
			tn = trie_cache->children[key];
		}

		// Remove first letter
		std::string subtoken = token.substr(1);

		std::vector<int> retrieved = find_token(tn, subtoken);
		std::set<int> unique_retrieved(retrieved.begin(), retrieved.end());
		for (int index : unique_retrieved) {
			if (results.find(index) == results.end()) {
				results[index] = 1;
			}
			else {
				results[index] = results[index] + 1;
			}
		}
	}

	std::vector<ScoredResult> ret;
	for (auto const& result : results) {
		ret.push_back({
			result.first,
			(float)result.second
			});
	}
	std::sort(ret.begin(), ret.end(), [](const ScoredResult& a, const ScoredResult& b) {
		return a.score > b.score;
		});

	return ret;
}


void ff_search(const std::string& table, const std::string& column, const std::string& query, unsigned int limit = 100, bool fuzzy = false) {
	std::cout << "Searching for '" << query << (fuzzy ? "~" : "") << "' in '" << table << ":" << column << "'" << std::endl;

	auto findAllstart = high_resolution_clock::now();
	auto results = find_all_tokens(query, table, column, fuzzy);
	auto findAllstop = high_resolution_clock::now();

	auto findAllduration = duration_cast <milliseconds> (findAllstop - findAllstart);


	std::string resultString = "";

	if (results.size() > 0) {
		unsigned int itter = 0;

		for (auto const& result : results) {

			auto line = read_cache(table, result.index);
			auto obj = split(line, ',');

			resultString += "    " + padd(obj[1], 24) + " (" + std::to_string(result.score) + ")    " + padd(obj[2], 12) + "    " + padd(obj[3], 24) + "    " + padd(obj[4], 24) + "    " + obj[0] + "\n";

			++itter;

			if (itter >= limit) {
				resultString += "    ...\n";
				break;
			}
		}
	}

	auto totalstop = high_resolution_clock::now();
	auto totalduration = duration_cast <milliseconds> (totalstop - findAllstart);

	std::string resultStats = "Found " + std::to_string(results.size()) + " results in " + std::to_string(totalduration.count()) + "ms (index: " + std::to_string(findAllduration.count()) + "ms)";

	std::cout << resultStats << "\n" << resultString << std::endl;
}

void ff_search_end() {
	// cleanup
	for (auto const& page : pageMap) {
		close_fast_file(page.second._fc);
	}
	delete trie_cache;
}





int main(int argc, char* argv[]) {

	if (argc > 1) {
		if (argv[1] == std::string("index")) {

			std::string csv_file = "";
			std::set<std::string> columns;

			for (int i = 1; i < argc; ++i) {
				if (argv[i] == std::string("-f")) {
					if (i + 1 > argc - 1) {
						std::cerr << "Error: no csv provided after -f flag" << std::endl;
						return 1;
					}
					csv_file = argv[i + 1];
				}
				else if (argv[i] == std::string("-c")) {
					if (i + 1 > argc - 1) {
						std::cerr << "Error: no columns provided after -c flag" << std::endl;
						return 1;
					}
					auto raw_columns = split(argv[i + 1], ',');
					columns = std::set(raw_columns.begin(), raw_columns.end());
				}
			}


			if (ff_index(csv_file, columns)) {
				std::cout << "Indexing sucessfully created" << std::endl;
			}
			else {
				std::cerr << "Indexing failed!" << std::endl;
			}

			return 0;
		}
		else if (argv[1] == std::string("search")) {

			std::string table_name = "";
			std::string column_name = "";
			std::string search_query = "";
			unsigned int limit = 10;
			bool fuzzy = false;

			for (int i = 1; i < argc; ++i) {
				if (argv[i] == std::string("-t")) {
					if (i + 1 > argc - 1) {
						std::cerr << "Error: no table name provided after -t flag" << std::endl;
						return 1;
					}
					table_name = argv[i + 1];
				}
				else if (argv[i] == std::string("-c")) {
					if (i + 1 > argc - 1) {
						std::cerr << "Error: no column name provided after -c flag" << std::endl;
						return 1;
					}
					column_name = argv[i + 1];
				}
				else if (argv[i] == std::string("-s")) {
					if (i + 1 > argc - 1) {
						std::cerr << "Error: no search query provided after -s flag" << std::endl;
						return 1;
					}
					search_query = argv[i + 1];
				}
				else if (argv[i] == std::string("-l")) {
					if (i + 1 > argc - 1) {
						std::cerr << "Error: no limit provided after -l flag" << std::endl;
						return 1;
					}
					limit = std::stoi(argv[i + 1]);
				}
				else if (argv[i] == std::string("-f")) {
					fuzzy = true;
				}
			}

			if (table_name == "" || column_name == "" || search_query == "") {
				std::cerr << "Error: Missing properties. Table name (-t), column name (-c) and search query (-s) have to be provided." << std::endl;
				return 1;
			}

			ff_search(table_name, column_name, search_query, limit, fuzzy);
			ff_search_end();

			return 0;
		}
	}
	else {
		std::string userIn = "";
		while (true) {
			std::cout << "Waiting for input" << std::endl;
			getline(std::cin, userIn);

			if (userIn == std::string("-x")) break;
			ff_search("names", "full_name", userIn);
		}
		ff_search_end();
	}


	std::cout << "Done\n";
	return 0;
}