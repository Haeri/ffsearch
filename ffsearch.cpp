#include <iostream>
#include <unordered_map>
#include <vector>
#include <set>
#include <string>
#include <sstream>
#include <cstring>
#include <fstream>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <string_view>

#ifdef _WIN32
#define NOMINMAX
#include <Windows.h>
#else
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#endif

#include "unidecode.h"


#define PAGE_SIZE 1000000

namespace fs = std::filesystem;
using namespace std::chrono;



struct FileContents {
	size_t size;
	char* buffer;
#ifdef _WIN32
	LPVOID _mapped;
	HANDLE _hMapping;
	HANDLE _hFile;
#else
	void* _mapped;
#endif
};


struct TreeNode
{
	std::unordered_map <unsigned char, TreeNode*> children;
	std::vector<int> index;
};

struct TablePage {
	size_t row_size;
	size_t num_rows;
	char* buffer;

	FileContents _fc;
};

struct SchemaColumnIndex {
	std::string column;
	int index;
	TreeNode* root;
};

struct ScoredResult {
	int index;
	float score;
};



const std::string PAGE_PREFIX = "page_";
const std::string TABLE_DIR = "tables";

const char SINGLE_WILDCARD = '?';
const char MULTI_WILDCARD = '*';

static std::unordered_map<int, TablePage> loaded_page_map;
static TreeNode* trie_cache;



#ifdef _WIN32
FileContents open_fast_read(const std::string& file_path) {
	std::ifstream file(file_path);
	if (!file) {
		std::cerr << "Error: Failed to open file." << std::endl;
		return {};
	}

	FileContents fc{};

	// Open the file for reading and mapping into memory
	fc._hFile = CreateFile(file_path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
	if (fc._hFile == INVALID_HANDLE_VALUE) {
		std::cerr << "Error: Failed to open file for memory mapping." << std::endl;
		return {};
	}
	fc._hMapping = CreateFileMapping(fc._hFile, nullptr, PAGE_READONLY, 0, 0, nullptr);
	if (fc._hMapping == nullptr) {
		std::cerr << "Error: Failed to map file into memory." << std::endl;
		CloseHandle(fc._hFile);
		return {};
	}
	fc._mapped = MapViewOfFile(fc._hMapping, FILE_MAP_READ, 0, 0, 0);
	if (fc._mapped == nullptr) {
		std::cerr << "Error: Failed to map file into memory." << std::endl;
		CloseHandle(fc._hMapping);
		CloseHandle(fc._hFile);
		return {};
	}

	fc.size = GetFileSize(fc._hFile, nullptr);
	fc.buffer = static_cast<char*>(fc._mapped);

	return fc;
}
void close_fast_read(FileContents fc) {
	UnmapViewOfFile(fc._mapped);
	CloseHandle(fc._hMapping);
	CloseHandle(fc._hFile);
}

#else
FileContents open_fast_read(const std::string& file_path) {
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

void close_fast_read(FileContents fc) {
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

std::string& pad_string(std::string& text, int size) {
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

std::string normalize_string(const std::string& text) {
	std::string output;
	std::string tmp;

	// UTF8 -> ASCII
	unidecode::Utf8StringIterator begin = text.c_str();
	unidecode::Utf8StringIterator end = text.c_str() + strlen(text.c_str());
	unidecode::Unidecode(begin, end, std::back_inserter(tmp));

	// Lowercase
	tmp = to_lowercase(tmp);

	for (const auto& c : tmp) {
		// Special Character to Whitespace
		if (c == ' ' || c == '-' || c == '+' || c == '@' || c == '\'' || c == '`') {
			output += ' ';
		}
		// keep [a-Z0-9]
		else if ((c >= 97 && c <= 122) || (c >= 48 && c <= 57)) {
			output += c;
		}
		else {
			if (c != '\'')
				std::cout << "Remove " << c << " from " << text << " -> " << tmp << " -> " << output << std::endl;
		}
	}

	return output;
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

		auto* child = new TreeNode();
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
	auto* root = new TreeNode();
	deserialize_node(root, infile);
	return root;
}



void insert_token(TreeNode* root, const std::string& token, int index)
{
	TreeNode* tnp = root;

	for (const char& i : token) {
		TreeNode* tn;
		unsigned char key = i;

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

	for (int i = 0; i < token.size(); ++i) {
		unsigned char key = token[i];

		if (key == SINGLE_WILDCARD) {
			for (auto const& child : tnp->children) {
				auto tmp = find_token(child.second, token.substr(i + 1));
				ret.insert(ret.end(), tmp.begin(), tmp.end());
			}
			return { ret.begin(), ret.end() };
		}
		else if (key == MULTI_WILDCARD) {
			for (auto const& child : tnp->children) {
				auto tmp = find_token(child.second, SINGLE_WILDCARD + token.substr(i + 1));
				ret.insert(ret.end(), tmp.begin(), tmp.end());

				tmp = find_token(child.second, MULTI_WILDCARD + token.substr(i + 1));
				ret.insert(ret.end(), tmp.begin(), tmp.end());
			}
			return  { ret.begin(), ret.end() };
		}
		else if (tnp->children.find(key) == tnp->children.end()) {
			return {};
		}
		else {
			tnp = tnp->children[key];
		}
	}

	ret.insert(ret.end(), tnp->index.begin(), tnp->index.end());

	return ret;
}



void generate_table(const std::string& path, const std::vector<std::string>& table) {
	for (int i = 0; i <= table.size() / PAGE_SIZE; ++i) {
		std::string table_page_file_name = std::string(path).append("/").append(PAGE_PREFIX).append(std::to_string(i));
		std::ofstream outfile(table_page_file_name, std::ios::binary);

		size_t max_row_size = 0;
		size_t row_count = std::min(PAGE_SIZE, (int)table.size() - i * PAGE_SIZE);
		for (int j = 0; j < row_count; ++j) {
			size_t table_index = j + i * (size_t)PAGE_SIZE;
			if (table[table_index].size() > max_row_size) max_row_size = table[table_index].size();
		}
		max_row_size += 1; // \0

		char* chunks = new char[max_row_size * row_count]();

		size_t offset = 0;
		for (size_t j = 0; j < row_count; ++j) {
			size_t table_index = j + i * (size_t)PAGE_SIZE;
#ifdef _WIN32
			strncat_s(chunks + offset, table[table_index].size() + 1, table[table_index].c_str(), max_row_size);
#else
			strncat(chunks + offset, table[table_index].c_str(), max_row_size);
#endif
			offset += max_row_size;
		}

		outfile.write(reinterpret_cast<const char*>(&max_row_size), sizeof(max_row_size));
		outfile.write(reinterpret_cast<const char*>(&row_count), sizeof(row_count));
		outfile.write(reinterpret_cast<const char*>(chunks), sizeof(char) * max_row_size * row_count);

		outfile.close();
	}
}

bool ff_index(const std::string& filename, const std::set<std::string>& columns) {
	std::cout << "Indexing started for " << filename << std::endl;

	std::string table_name = fs::path(filename).stem().string();
	std::string table_path = TABLE_DIR + "/" + table_name;

	if (!std::filesystem::is_directory(table_path) || !std::filesystem::exists(table_path)) {
		std::filesystem::create_directories(table_path);
	}

	std::vector<std::string> table;
	std::vector<SchemaColumnIndex> schema_column_index;

	std::string line;

	std::ifstream scv_file(filename);
	int line_index = 0;

	if (!scv_file.is_open()) {
		std::cerr << "Error: Could not open the file " << filename << std::endl;
		return false;
	}

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
		std::vector<std::string> row_elements = split(line, ',');

		for (auto const& index_column : schema_column_index) {

			std::string element = normalize_string(row_elements[index_column.index]);
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


	for (auto const& index_column : schema_column_index) {
		std::string dir_name = table_path + "/index/" + index_column.column + "/trie/";
		if (!std::filesystem::is_directory(dir_name) || !std::filesystem::exists(dir_name)) {
			std::filesystem::create_directories(dir_name);
		}
		for (auto const& alphabet_index : index_column.root->children) {
			serialize_trie(alphabet_index.second, dir_name + std::to_string(alphabet_index.first));
		}
	}

	generate_table(table_path, table);

	return true;
}


std::string read_table(const std::string& table, int index) {
	int page = index / PAGE_SIZE;
	int line = index - (page * PAGE_SIZE);

	TablePage table_page{};

	if (loaded_page_map.find(page) == loaded_page_map.end()) {
		table_page._fc = open_fast_read(TABLE_DIR + "/" + table + "/" + PAGE_PREFIX + std::to_string(page));

		table_page.row_size = *((size_t*)table_page._fc.buffer);
		table_page.num_rows = *((size_t*)(table_page._fc.buffer + sizeof(size_t)));
		table_page.buffer = (table_page._fc.buffer + 2 * sizeof(size_t));

		loaded_page_map[page] = table_page;
	}
	else {
		table_page = loaded_page_map[page];
	}

	return { table_page.buffer + (line * table_page.row_size) }; // , tp.row_size);
}

std::vector<ScoredResult> find_all_tokens(const std::string& input, const std::string& table, const std::string& column, bool fuzzy) {
	if (trie_cache == nullptr)
		trie_cache = new TreeNode;

	std::unordered_map<int, int> results;
	std::vector<std::string> tokens;

	std::string normalized_input = normalize_string(input);

	if (fuzzy) {
		std::vector<std::string> _tokens = split(normalized_input, ' ');

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
	}
	else {
		tokens = split(normalized_input, ' ');
	}


	for (const std::string& token : tokens) {
		unsigned char key = token[0];

		TreeNode* tree_node;

		if (trie_cache->children.find(key) == trie_cache->children.end()) {
			tree_node = deserialize_trie(std::string(TABLE_DIR).append("/").append(table).append("/index/").append(column).append("/trie/").append(std::to_string(key)));
			if (tree_node == nullptr) continue;
			trie_cache->children[key] = tree_node;
		}
		else {
			tree_node = trie_cache->children[key];
		}

		// Remove first letter
		std::string sub_token = token.substr(1);

		std::vector<int> retrieved = find_token(tree_node, sub_token);
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

	auto start = high_resolution_clock::now();
	auto results = find_all_tokens(query, table, column, fuzzy);
	auto find_all_tokens_stop = high_resolution_clock::now();

	auto find_all_tokens_duration = duration_cast <milliseconds> (find_all_tokens_stop - start);


	std::string result_string;

	if (!results.empty()) {
		unsigned int itter = 0;

		for (auto const& result : results) {

			auto line = read_table(table, result.index);
			auto obj = split(line, ',');

			result_string += "    " + pad_string(obj[1], 24) + " (" + std::to_string(result.score) + ")    " +
				pad_string(obj[2], 12) + "    " +
				pad_string(obj[3], 24) + "    " +
				pad_string(obj[4], 24) + "    " + obj[0] + "\n";

			++itter;

			if (itter >= limit) {
				result_string += "    ...\n";
				break;
			}
		}
	}

	auto total_stop = high_resolution_clock::now();
	auto total_duration = duration_cast <milliseconds> (total_stop - start);

	std::string result_stats = "Found " + std::to_string(results.size()) + " results in " + std::to_string(total_duration.count()) + "ms (index: " + std::to_string(find_all_tokens_duration.count()) + "ms)";

	std::cout << result_stats << "\n" << result_string << std::endl;
}

void ff_search_end() {
	// cleanup
	for (auto const& page : loaded_page_map) {
		close_fast_read(page.second._fc);
	}
	delete trie_cache;
}


void ff_insert(const std::string& table, const std::set<std::string>& columns) {


}




int main(int argc, char* argv[]) {

#ifdef _WIN32
	SetConsoleOutputCP(65001);
#endif


	if (argc > 1) {
		if (argv[1] == std::string("index")) {

			std::string csv_file;
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

			std::string table_name;
			std::string column_name;
			std::string search_query;
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

			if (table_name.empty() || column_name.empty() || search_query.empty()) {
				std::cerr << "Error: Missing properties. Table name (-t), column name (-c) and search query (-s) have to be provided." << std::endl;
				return 1;
			}

			ff_search(table_name, column_name, search_query, limit, fuzzy);
			ff_search_end();

			return 0;
		}
	}
	else {
		std::string user_in;
		while (true) {
			std::cout << "Waiting for input" << std::endl;
			getline(std::cin, user_in);

			if (user_in == std::string("-x")) break;
			ff_search("names", "full_name", user_in);
		}
		ff_search_end();
	}


	std::cout << "Done\n";
	return 0;
}