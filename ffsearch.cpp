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
#include <thread>
#include <mutex>
#include <condition_variable>

#ifdef _WIN32
#ifndef __MINGW32__
#define NOMINMAX
#endif
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

struct FileContents
{
	size_t size;
	char *buffer;
#ifdef _WIN32
	LPVOID _mapped;
	HANDLE _hMapping;
	HANDLE _hFile;
#else
	void *_mapped;
#endif
};

struct TreeNode
{
	std::unordered_map<unsigned char, TreeNode *> children;
	std::vector<int> index;
};

struct TablePage
{
	size_t row_size;
	size_t num_rows;
	char *buffer;

	FileContents _fc;
};

struct SchemaColumnIndex
{
	std::string column;
	int index;
	TreeNode *root;
};

struct ScoredResult
{
	int index;
	float score;
};

const std::string PAGE_PREFIX = "page_";
const std::string TABLE_DIR = "tables";
const std::string SCHEMA_FILE = "schema";

const char SINGLE_WILDCARD = '?';
const char MULTI_WILDCARD = '*';

static std::unordered_map<int, TablePage> loaded_page_map;
static TreeNode *trie_cache;

#ifdef _WIN32
FileContents open_fast_read(const std::string &file_path)
{
	std::ifstream file(file_path);
	if (!file)
	{
		std::cerr << "Error: Failed to open file." << std::endl;
		return {};
	}

	FileContents fc{};

	// Open the file for reading and mapping into memory
	fc._hFile = CreateFileA(file_path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
	if (fc._hFile == INVALID_HANDLE_VALUE)
	{
		std::cerr << "Error: Failed to open file for memory mapping." << std::endl;
		return {};
	}
	fc._hMapping = CreateFileMapping(fc._hFile, nullptr, PAGE_READONLY, 0, 0, nullptr);
	if (fc._hMapping == nullptr)
	{
		std::cerr << "Error: Failed to map file into memory." << std::endl;
		CloseHandle(fc._hFile);
		return {};
	}
	fc._mapped = MapViewOfFile(fc._hMapping, FILE_MAP_READ, 0, 0, 0);
	if (fc._mapped == nullptr)
	{
		std::cerr << "Error: Failed to map file into memory." << std::endl;
		CloseHandle(fc._hMapping);
		CloseHandle(fc._hFile);
		return {};
	}

	fc.size = GetFileSize(fc._hFile, nullptr);
	fc.buffer = static_cast<char *>(fc._mapped);

	return fc;
}
void close_fast_read(FileContents fc)
{
	UnmapViewOfFile(fc._mapped);
	CloseHandle(fc._hMapping);
	CloseHandle(fc._hFile);
}

#else
FileContents open_fast_read(const std::string &file_path)
{
	int fd = open(file_path.c_str(), O_RDONLY);
	if (fd == -1)
	{
		std::cerr << "Error: Failed to open file." << std::endl;
		return {};
	}

	struct stat st;
	if (fstat(fd, &st) == -1)
	{
		std::cerr << "Error: Failed to get file size." << std::endl;
		close(fd);
		return {};
	}

	FileContents fc{};
	fc.size = static_cast<size_t>(st.st_size);

	// Map the file into memory
	fc._mapped = mmap(nullptr, fc.size, PROT_READ, MAP_PRIVATE, fd, 0);
	if (fc._mapped == MAP_FAILED)
	{
		std::cerr << "Error: Failed to map file into memory." << std::endl;
		close(fd);
		return {};
	}
	fc.buffer = static_cast<char *>(fc._mapped);

	close(fd);
	return fc;
}

void close_fast_read(FileContents fc)
{
	// Unmap the file from memory
	munmap(fc._mapped, fc.size);
}
#endif

std::vector<std::string> split(const std::string &input, const char &delimiter)
{
	std::vector<std::string> elements;
	elements.reserve(std::count(input.begin(), input.end(), delimiter) + 1); // reserve space for all expected elements
	std::stringstream stream(input);
	std::string element;

	while (getline(stream, element, delimiter))
	{
		elements.push_back(std::move(element));
	}

	return elements;
}

size_t utf8_strlen(const std::string &utf8_string)
{
	size_t length = 0;
	for (size_t i = 0; i < utf8_string.length();)
	{
		unsigned char c = utf8_string[i];
		if (c < 0x80)
		{ // ASCII character
			length++;
			i++;
		}
		else if (c < 0xE0)
		{ // 2-byte sequence
			length++;
			i += 2;
		}
		else if (c < 0xF0)
		{ // 3-byte sequence
			length++;
			i += 3;
		}
		else
		{ // 4-byte sequence
			length++;
			i += 4;
		}
	}
	return length;
}

std::string &pad_string(std::string &text, int size)
{
	if (utf8_strlen(text) < size)
	{
		text.insert(text.end(), size - utf8_strlen(text), ' ');
	}
	else
	{
		text.replace(size - 3, 3, "...");
		text = text.substr(0, size);
	}
	return text;
}

std::string to_lowercase(const std::string &s)
{
	std::string result = s;
	std::transform(result.begin(), result.end(), result.begin(),
				   [](unsigned char c)
				   { return std::tolower(c); });
	return result;
}

// Function to remove leading and trailing whitespaces from a string
std::string trim(const std::string &str)
{
	size_t start = 0;
	size_t end = str.length() - 1;

	while (start <= end && std::isspace(str[start]))
		++start;

	while (end >= start && std::isspace(str[end]))
		--end;

	return str.substr(start, end - start + 1);
}

std::string escape_json_string(const std::string &input)
{
	std::ostringstream ss;
	ss << std::quoted(input, '"', '\\');
	std::string escaped = ss.str();

	// std::quoted adds extra quotes around the string, remove them
	if (!escaped.empty() && escaped.front() == '"' && escaped.back() == '"')
	{
		escaped = escaped.substr(1, escaped.size() - 2);
	}

	// Replace newlines, tabs, and other control characters
	std::string result;
	for (char c : escaped)
	{
		switch (c)
		{
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		default:
			if ('\x00' <= c && c <= '\x1f')
			{
				ss.str("");
				ss.clear();
				ss << "\\u" << std::setw(4) << std::setfill('0') << std::hex << std::uppercase << static_cast<int>(c);
				result += ss.str();
			}
			else
			{
				result += c;
			}
		}
	}

	return result;
}

// Function to remove multiple whitespaces from a string
std::string removeExtraSpaces(const std::string &str)
{
	std::string result;
	result.reserve(str.size()); // Reserve space to avoid dynamic resizing

	bool previousIsSpace = false;
	for (char c : str)
	{
		if (!std::isspace(c))
		{
			result.push_back(c);
			previousIsSpace = false;
		}
		else
		{
			if (!previousIsSpace)
			{
				result.push_back(' ');
				previousIsSpace = true;
			}
		}
	}
	return result;
}

std::string normalize_string(const std::string &text)
{
	std::string output;
	std::string tmp;

	// UTF8 -> ASCII
	unidecode::Utf8StringIterator begin = text.c_str();
	unidecode::Utf8StringIterator end = text.c_str() + strlen(text.c_str());
	unidecode::Unidecode(begin, end, std::back_inserter(tmp));

	// Lowercase
	tmp = to_lowercase(tmp);

	for (const auto &c : tmp)
	{
		// Special Character to Whitespace
		if (c == ' ' || c == '-' || c == '+' || c == '@' || c == '\'' || c == '`')
		{
			output += ' ';
		}
		// keep [a-Z0-9]
		else if ((c >= 97 && c <= 122) || (c >= 48 && c <= 57))
		{
			output += c;
		}
		else
		{
			// if (c != '\'')
			//	std::cout << "Remove " << c << " from " << text << " -> " << tmp << " -> " << output << std::endl;
		}
	}

	output = trim(removeExtraSpaces(output));

	return output;
}

template <typename... Args>
void log_debug(const Args &...args)
{
	return;
	{
		std::ostringstream ss;
		(ss << ... << args) << "\n"; // Fold expression for variadic templates
		std::cout << ss.str();
	}
}

void err_out(const std::string &err_message)
{
	std::cout << "{\n";
	std::cout << "  \"status\": \"error\",\n";
	std::cout << "  \"message\": \"Error: " << err_message << "\"\n";
	std::cout << "}" << std::endl;
	exit(1);
}

void serialize_node(TreeNode *node, std::ofstream &outfile)
{
	// write the number of children
	size_t num_children = node->children.size();
	outfile.write(reinterpret_cast<const char *>(&num_children), sizeof(num_children));

	// write the indices associated with this node
	size_t num_indices = node->index.size();
	outfile.write(reinterpret_cast<const char *>(&num_indices), sizeof(num_indices));
	for (int i : node->index)
	{
		outfile.write(reinterpret_cast<const char *>(&i), sizeof(i));
	}

	// recursively serialize each child node
	for (auto &entry : node->children)
	{
		outfile.write(reinterpret_cast<const char *>(&entry.first), sizeof(entry.first));
		serialize_node(entry.second, outfile);
	}
}

void serialize_trie(TreeNode *root, const std::string &filename)
{
	std::cout << "Create tree " << filename << std::endl;
	std::ofstream outfile(filename, std::ios::binary);
	if (!outfile.is_open())
	{
		std::cerr << "Error: could not write to file " << filename << std::endl;
		return;
	}
	serialize_node(root, outfile);
}

void deserialize_node(TreeNode *node, std::ifstream &infile)
{
	// read the number of children
	size_t num_children;
	infile.read(reinterpret_cast<char *>(&num_children), sizeof(num_children));

	// read the indices associated with this node
	size_t num_indices;
	infile.read(reinterpret_cast<char *>(&num_indices), sizeof(num_indices));
	if (num_indices > 0)
	{
		node->index.resize(num_indices);
		infile.read(reinterpret_cast<char *>(&(node->index[0])), sizeof(int) * num_indices);
	}

	// recursively deserialize each child node
	for (size_t i = 0; i < num_children; i++)
	{
		unsigned char key;
		infile.read(reinterpret_cast<char *>(&key), sizeof(key));

		auto *child = new TreeNode();
		node->children[key] = child;
		deserialize_node(child, infile);
	}
}

TreeNode *deserialize_trie(const std::string &filename)
{
	log_debug(std::this_thread::get_id(), " deserialize_trie", filename);

	std::ifstream infile(filename, std::ios::binary);
	if (!infile.is_open())
	{
		std::cerr << "Error: could not open file " << filename << std::endl;
		return nullptr;
	}
	auto *root = new TreeNode();
	deserialize_node(root, infile);
	return root;
}

void serialize_schema(const std::vector<std::string> &schema, const std::string &table_name)
{
	std::string schema_file_path = TABLE_DIR + "/" + table_name + "/" + SCHEMA_FILE;

	// Open the file in binary mode to ensure proper newline handling
	std::ofstream outFile(schema_file_path, std::ios::binary);
	if (!outFile.is_open())
	{
		// Failed to open file, handle error
		std::cerr << "Error: Unable to open file for writing\n";
		return;
	}

	// Write each string from the vector to the file followed by a newline character
	for (const std::string &str : schema)
	{
		outFile << str << '\n';
	}

	// Close the file
	outFile.close();
}

std::vector<std::string> deserialize_schema(const std::string &table_name)
{
	std::string schema_file_path = TABLE_DIR + "/" + table_name + "/" + SCHEMA_FILE;

	std::ifstream file(schema_file_path);
	std::vector<std::string> names;
	std::string line;
	while (std::getline(file, line))
	{
		names.push_back(line);
	}
	return names;
}

void insert_token(TreeNode *root, const std::string &token, int index)
{
	TreeNode *tnp = root;

	for (const char &i : token)
	{
		TreeNode *tn;
		unsigned char key = i;

		if (tnp->children.find(key) == tnp->children.end())
		{
			tn = new TreeNode();
		}
		else
		{
			tn = tnp->children[key];
		}

		tnp->children[key] = tn;
		tnp = tn;
	}
	tnp->index.push_back(index);
}

void generate_table(const std::string &path, const std::vector<std::string> &table)
{
	for (int i = 0; i <= table.size() / PAGE_SIZE; ++i)
	{
		std::string table_page_file_name = std::string(path).append("/").append(PAGE_PREFIX).append(std::to_string(i));
		std::ofstream outfile(table_page_file_name, std::ios::binary);

		size_t max_row_size = 0;
		size_t row_count = std::min(PAGE_SIZE, (int)table.size() - i * PAGE_SIZE);
		for (int j = 0; j < row_count; ++j)
		{
			size_t table_index = j + i * (size_t)PAGE_SIZE;
			if (table[table_index].size() > max_row_size)
				max_row_size = table[table_index].size();
		}
		max_row_size += 1; // \0

		char *chunks = new char[max_row_size * row_count]();

		size_t offset = 0;
		for (size_t j = 0; j < row_count; ++j)
		{
			size_t table_index = j + i * (size_t)PAGE_SIZE;
#ifdef _WIN32
			strncat_s(chunks + offset, table[table_index].size() + 1, table[table_index].c_str(), max_row_size);
#else
			strncat(chunks + offset, table[table_index].c_str(), max_row_size);
#endif
			offset += max_row_size;
		}

		outfile.write(reinterpret_cast<const char *>(&max_row_size), sizeof(max_row_size));
		outfile.write(reinterpret_cast<const char *>(&row_count), sizeof(row_count));
		outfile.write(reinterpret_cast<const char *>(chunks), sizeof(char) * max_row_size * row_count);

		outfile.close();
		delete[] chunks;
	}
}

bool ff_index(const std::string &filename, const std::set<std::string> &columns)
{
	std::cout << "Indexing started for " << filename << std::endl;

	std::string table_name = fs::path(filename).stem().string();
	std::string table_path = TABLE_DIR + "/" + table_name;

	if (!std::filesystem::is_directory(table_path) || !std::filesystem::exists(table_path))
	{
		std::filesystem::create_directories(table_path);
	}

	std::vector<std::string> table;
	std::vector<SchemaColumnIndex> schema_column_index;

	std::string line;

	std::ifstream scv_file(filename);
	int line_index = 0;

	if (!scv_file.is_open())
	{
		std::cerr << "Error: Could not open the file " << filename << std::endl;
		return false;
	}

	// Get schema
	getline(scv_file, line);
	std::vector<std::string> schema = split(line, ',');

	int schema_index = 0;
	for (auto const &column : schema)
	{
		if (columns.find(column) != columns.end())
		{
			schema_column_index.push_back({column,
										   schema_index,
										   new TreeNode});
		}
		++schema_index;
	}

	while (getline(scv_file, line))
	{

		table.push_back(line);
		std::vector<std::string> row_elements = split(line, ',');

		for (auto const &index_column : schema_column_index)
		{

			std::string element = normalize_string(row_elements[index_column.index]);
			std::vector<std::string> element_parts = split(element, ' ');
			std::set<std::string> unique_element_parts(element_parts.begin(), element_parts.end());

			for (auto const &unique_element_part : unique_element_parts)
			{
				insert_token(index_column.root, unique_element_part, line_index);
			}
		}

		++line_index;
		if (line_index % PAGE_SIZE == 0)
		{
			std::cout << "Indexing " << line_index << std::endl;
		}
	}
	scv_file.close();

	for (auto const &index_column : schema_column_index)
	{
		std::string dir_name = table_path + "/index/" + index_column.column + "/trie/";
		if (!std::filesystem::is_directory(dir_name) || !std::filesystem::exists(dir_name))
		{
			std::filesystem::create_directories(dir_name);
		}
		for (auto const &alphabet_index : index_column.root->children)
		{
			serialize_trie(alphabet_index.second, dir_name + std::to_string(alphabet_index.first));
		}
	}

	serialize_schema(schema, table_name);
	generate_table(table_path, table);

	return true;
}

std::string read_table(const std::string &table, int index)
{
	int page = index / PAGE_SIZE;
	int line = index - (page * PAGE_SIZE);

	TablePage table_page{};

	if (loaded_page_map.find(page) == loaded_page_map.end())
	{
		table_page._fc = open_fast_read(TABLE_DIR + "/" + table + "/" + PAGE_PREFIX + std::to_string(page));

		table_page.row_size = *((size_t *)table_page._fc.buffer);
		table_page.num_rows = *((size_t *)(table_page._fc.buffer + sizeof(size_t)));
		table_page.buffer = (table_page._fc.buffer + 2 * sizeof(size_t));

		loaded_page_map[page] = table_page;
	}
	else
	{
		table_page = loaded_page_map[page];
	}

	return {table_page.buffer + (line * table_page.row_size)}; // , tp.row_size);
}

std::vector<int> find_token(TreeNode *root, const std::string &token)
{
	log_debug(std::this_thread::get_id(), " find_token ", token);

	TreeNode *tnp = root;
	std::vector<int> ret;

	for (int i = 0; i < token.size(); ++i)
	{
		unsigned char key = token[i];

		if (key == SINGLE_WILDCARD)
		{
			for (auto const &child : tnp->children)
			{
				auto tmp = find_token(child.second, token.substr(i + 1));
				ret.insert(ret.end(), tmp.begin(), tmp.end());
			}
			return {ret.begin(), ret.end()};
		}
		else if (key == MULTI_WILDCARD)
		{
			for (auto const &child : tnp->children)
			{
				auto tmp = find_token(child.second, SINGLE_WILDCARD + token.substr(i + 1));
				ret.insert(ret.end(), tmp.begin(), tmp.end());

				tmp = find_token(child.second, MULTI_WILDCARD + token.substr(i + 1));
				ret.insert(ret.end(), tmp.begin(), tmp.end());
			}
			return {ret.begin(), ret.end()};
		}
		else if (tnp->children.find(key) == tnp->children.end())
		{
			return {};
		}
		else
		{
			tnp = tnp->children[key];
		}
	}

	ret.insert(ret.end(), tnp->index.begin(), tnp->index.end());

	return ret;
}

std::mutex cache_mutex;
std::condition_variable cache_cv;
std::unordered_map<unsigned char, bool> cache_in_progress;

std::vector<int> find_token_root(const std::string &table, const std::string &column, const std::string &token)
{
	log_debug(std::this_thread::get_id(), " find_token_root ", token);

	TreeNode *tree_node;

	unsigned char key = token[0];
	if (key == SINGLE_WILDCARD || key == MULTI_WILDCARD)
		return {}; // skip wildcard prefix for now

	{
		std::unique_lock<std::mutex> lock(cache_mutex);

		while (cache_in_progress[key])
		{
			// Wait if another thread is already deserializing this key
			cache_cv.wait(lock);
		}

		if (trie_cache->children.find(key) == trie_cache->children.end())
		{
			// Mark that we're deserializing this key
			cache_in_progress[key] = true;
			lock.unlock();

			// Perform deserialization outside the critical section
			tree_node = deserialize_trie(std::string(TABLE_DIR).append("/").append(table).append("/index/").append(column).append("/trie/").append(std::to_string(key)));

			lock.lock();
			if (tree_node == nullptr)
			{
				// Remove the progress marker and notify all waiting threads
				cache_in_progress.erase(key);
				cache_cv.notify_all();
				return {};
			}
			trie_cache->children[key] = tree_node;
			// Remove the progress marker
			cache_in_progress.erase(key);
			cache_cv.notify_all();
		}
		else
		{
			// If the key is already in the cache, use it directly
			tree_node = trie_cache->children[key];
		}
	}

	// Remove first letter
	std::string sub_token = token.substr(1);

	return find_token(tree_node, sub_token);
}

void find_one_token(const std::string &table, const std::string &column, const std::string &token, bool fuzzy, std::vector<ScoredResult> &results)
{
	log_debug(std::this_thread::get_id(), " find_one_token ", token);

	auto all_indices = find_token_root(table, column, token);
	results.reserve(all_indices.size());
	std::transform(all_indices.begin(), all_indices.end(), std::back_inserter(results),
				   [](int num)
				   { return ScoredResult{num, 100.0f}; });

	if (fuzzy)
	{
		float fuzzyScore = ((token.size() - 1.0f) / token.size()) * 100.0f;

		// Generate fuzzy variations
		std::vector<std::string> fuzzy_tokens;
		fuzzy_tokens.reserve(4 * token.size() + 1);

		for (int i = 0; i < token.size(); ++i)
		{
			fuzzy_tokens.push_back(token.substr(0, i) + '?' + token.substr(i));		// INSERTION
			fuzzy_tokens.push_back(token.substr(0, i) + token.substr(i + 1));		// DELETION
			fuzzy_tokens.push_back(token.substr(0, i) + '?' + token.substr(i + 1)); // SUBSTITUTION

			if (i < token.size() - 1)
			{ // TRANSPOSITION
				std::string cpy(token);
				std::swap(cpy[i], cpy[i + 1]);
				fuzzy_tokens.push_back(std::move(cpy));
			}
		}
		fuzzy_tokens.push_back(token + '?');

		// Collect all fuzzy results
		std::vector<int> all_fuzzy_indices;
		all_fuzzy_indices.reserve(all_indices.size());
		for (auto const &fuzzy_token : fuzzy_tokens)
		{
			auto tmp = find_token_root(table, column, fuzzy_token);
			all_fuzzy_indices.insert(all_fuzzy_indices.end(), std::make_move_iterator(tmp.begin()), std::make_move_iterator(tmp.end()));
		}

		// Sort the and remove dublicates
		std::sort(all_fuzzy_indices.begin(), all_fuzzy_indices.end());
		all_fuzzy_indices.erase(std::unique(all_fuzzy_indices.begin(), all_fuzzy_indices.end()), all_fuzzy_indices.end());
		all_fuzzy_indices.erase(std::remove_if(all_fuzzy_indices.begin(), all_fuzzy_indices.end(), [&all_indices](auto x)
											   { return std::find(all_indices.begin(), all_indices.end(), x) != all_indices.end(); }),
								all_fuzzy_indices.end());

		std::transform(all_fuzzy_indices.begin(), all_fuzzy_indices.end(), std::back_inserter(results),
					   [&fuzzyScore](int num)
					   { return ScoredResult{num, fuzzyScore}; });
	}
}

std::vector<ScoredResult> find_all_tokens(const std::string &table, const std::string &column, const std::string &input, bool and_op, bool fuzzy)
{
	if (trie_cache == nullptr)
		trie_cache = new TreeNode;

	std::string normalized_input = normalize_string(input);
	std::vector<std::string> tokens = split(normalized_input, ' ');

	size_t token_count = tokens.size();
	std::vector<std::vector<ScoredResult>> thread_results(token_count);
	std::vector<std::thread> threads;
	threads.reserve(token_count);

	for (size_t i = 0; i < token_count; ++i)
	{
		threads.emplace_back(find_one_token, table, column, std::cref(tokens[i]), fuzzy, std::ref(thread_results[i]));
	}
	for (auto &t : threads)
	{
		t.join();
	}

	struct score_and_count
	{
		float score = 0.0f;
		int count = 0;
	};

	std::unordered_map<int, score_and_count> score_sum;

	for (const auto &results_arr : thread_results)
	{
		for (const ScoredResult &sr : results_arr)
		{
			auto &entry = score_sum[sr.index];
			entry.score += sr.score;
			entry.count += 1;
		}
	}

	std::vector<ScoredResult> ret;
	ret.reserve(score_sum.size());
	for (const auto &[index, snc] : score_sum)
	{
		if (and_op && snc.count < static_cast<int>(token_count))
			continue;
		ret.push_back({index, snc.score});
	}

	std::sort(ret.begin(), ret.end(), [](const ScoredResult &a, const ScoredResult &b)
			  { return a.score > b.score; });

	return ret;
}

void ff_search(const std::string &table, const std::string &column, const std::string &query, unsigned int limit = 100, bool and_op = false, bool fuzzy = false, unsigned int offset = 0)
{

	std::string table_path = TABLE_DIR + "/" + table;
	if (!std::filesystem::is_directory(table_path))
	{
		err_out("Table '" + table + "' not found.");
	}

	std::string column_path = TABLE_DIR + "/" + table + "/index/" + column;
	if (!std::filesystem::is_directory(column_path))
	{
		err_out("Column '" + column + "' not found.");
	}

	auto start = high_resolution_clock::now();
	auto results = find_all_tokens(table, column, query, and_op, fuzzy);
	auto find_all_tokens_stop = high_resolution_clock::now();

	auto find_all_tokens_duration = duration_cast<milliseconds>(find_all_tokens_stop - start);

	std::vector<std::string> schema;
	if (results.size() > 0)
	{
		schema = deserialize_schema(table);
	}

	std::cout << "{\n";
	std::cout << "  \"status\": \"ok\",\n";

	std::cout << "  \"input\": {\n";
	std::cout << "    \"table\": \"" << escape_json_string(table) << "\",\n";
	std::cout << "    \"column\": \"" << escape_json_string(column) << "\",\n";
	std::cout << "    \"query\": \"" << escape_json_string(query) << "\",\n";
	if (limit != 100)
		std::cout << "    \"limit\": " << limit << ",\n";
	if (offset != 0)
		std::cout << "    \"offset\": " << offset << ",\n";
	if (fuzzy)
		std::cout << "    \"fuzzy\": " << (fuzzy ? "true" : "false") << ",\n";
	if (and_op)
		std::cout << "    \"and_op\": " << (and_op ? "true" : "false") << ",\n";
	std::cout << "  },\n";

	std::cout << "  \"result\": {\n";
	std::cout << "    \"meta\": {\n";
	std::cout << "      \"total_results\": " << results.size() << ",\n";
	// std::cout << "      \"total_duration_ms\": " << find_all_tokens_duration.count() << ",\n";
	std::cout << "      \"lookup_duration_ms\": " << find_all_tokens_duration.count() << "\n";
	std::cout << "    },\n";
	std::cout << "    \"data\": [\n";

	for (size_t i = offset; i < std::min(results.size(), static_cast<size_t>(offset + limit)); ++i)
	{
		auto line = read_table(table, results[i].index);
		auto obj = split(line, ',');

		std::cout << "      {\n";
		for (size_t j = 0; j < schema.size(); ++j)
		{
			std::cout << "        \"" << escape_json_string(schema[j]) << "\": \"" << escape_json_string(obj[j]) << "\",\n";
		}

		std::cout << "        \"__score\": " << results[i].score << "\n";
		std::cout << "      }";

		if (i != std::min(results.size(), static_cast<size_t>(limit)) - 1)
			std::cout << ",";
		std::cout << "\n";
	}

	std::cout << "    ]\n";
	std::cout << "  }\n";
	std::cout << "}" << std::endl;
}

void ff_search_end()
{
	// cleanup
	for (auto const &page : loaded_page_map)
	{
		close_fast_read(page.second._fc);
	}
	delete trie_cache;
}

std::string get_latst_page(const std::string &table)
{
	int highestPageNum = -1;
	std::string highestPageFile = "";

	for (const auto &entry : std::filesystem::directory_iterator(TABLE_DIR + "/" + table))
	{
		if (entry.is_regular_file() && entry.path().filename().string().substr(0, 5) == "page_")
		{
			std::string filename = entry.path().filename().string();
			int pageNum = std::stoi(filename.substr(5));
			if (pageNum > highestPageNum)
			{
				highestPageNum = pageNum;
				highestPageFile = filename;
			}
		}
	}

	return highestPageFile;
}

void ff_insert(const std::string &table, const std::string &value)
{
	auto page = get_latst_page(table);
}

int main(int argc, char *argv[])
{

#ifdef _WIN32
	SetConsoleOutputCP(65001);
#endif

	// ff_index("names.csv", { "full_name", "nationality"});
	// ff_search("names", "full_name", "Peter", 100, false, true);
	// return 0;

	if (argc > 1)
	{
		if (argv[1] == std::string("index"))
		{
			std::string csv_file;
			std::set<std::string> columns;

			for (int i = 1; i < argc; ++i)
			{
				if (argv[i] == std::string("-f"))
				{
					if (i + 1 > argc - 1)
					{
						std::cerr << "Error: no csv provided after -f flag" << std::endl;
						return 1;
					}
					csv_file = argv[i + 1];
				}
				else if (argv[i] == std::string("-c"))
				{
					if (i + 1 > argc - 1)
					{
						std::cerr << "Error: no columns provided after -c flag" << std::endl;
						return 1;
					}
					auto raw_columns = split(argv[i + 1], ',');
					columns = std::set(raw_columns.begin(), raw_columns.end());
				}
			}

			if (csv_file.empty() || columns.empty())
			{
				std::cerr << "Error: Missing properties. File name (-f) and column names (-c) have to be provided." << std::endl;
				return 1;
			}

			if (ff_index(csv_file, columns))
			{
				std::cout << "Index successfully created" << std::endl;
			}
			else
			{
				std::cerr << "Indexing failed!" << std::endl;
				return 1;
			}
		}
		else if (argv[1] == std::string("insert"))
		{
			std::string table_name;
			std::string value;

			for (int i = 1; i < argc; ++i)
			{
				if (argv[i] == std::string("-t"))
				{
					if (i + 1 > argc - 1)
					{
						std::cerr << "Error: no table name provided after -t flag" << std::endl;
						return 1;
					}
					table_name = argv[i + 1];
				}
				else if (argv[i] == std::string("-v"))
				{
					if (i + 1 > argc - 1)
					{
						std::cerr << "Error: no value provided after -v flag" << std::endl;
						return 1;
					}
					value = argv[i + 1];
				}
			}

			if (table_name.empty() || value.empty())
			{
				std::cerr << "Error: Missing properties. Table name (-t) and value (-v) have to be provided." << std::endl;
				return 1;
			}

			ff_insert(table_name, value);
		}
		else if (argv[1] == std::string("search"))
		{

			std::string table_name;
			std::string column_name;
			std::string search_query;
			unsigned int limit = 10;
			unsigned int offset = 0;
			bool fuzzy = false;
			bool and_op = false;
			bool interactive = false;

			for (int i = 1; i < argc; ++i)
			{
				if (argv[i] == std::string("-t"))
				{
					if (i + 1 > argc - 1)
					{
						std::cerr << "Error: no table name provided after -t flag" << std::endl;
						return 1;
					}
					table_name = argv[i + 1];
				}
				else if (argv[i] == std::string("-c"))
				{
					if (i + 1 > argc - 1)
					{
						std::cerr << "Error: no column name provided after -c flag" << std::endl;
						return 1;
					}
					column_name = argv[i + 1];
				}
				else if (argv[i] == std::string("-s"))
				{
					if (i + 1 > argc - 1)
					{
						std::cerr << "Error: no search query provided after -s flag" << std::endl;
						return 1;
					}
					search_query = argv[i + 1];
				}
				else if (argv[i] == std::string("-l"))
				{
					if (i + 1 > argc - 1)
					{
						std::cerr << "Error: no limit value provided after -l flag" << std::endl;
						return 1;
					}
					limit = std::stoi(argv[i + 1]);
				}
				else if (argv[i] == std::string("-o"))
				{
					if (i + 1 > argc - 1)
					{
						std::cerr << "Error: no offset value provided after -o flag" << std::endl;
						return 1;
					}
					offset = std::stoi(argv[i + 1]);
				}
				else if (argv[i] == std::string("-a"))
				{
					and_op = true;
				}
				else if (argv[i] == std::string("-f"))
				{
					fuzzy = true;
				}
				else if (argv[i] == std::string("-i"))
				{
					interactive = true;
				}
			}

			if (table_name.empty() || column_name.empty() || (!interactive && search_query.empty()))
			{
				std::cerr << "Error: Missing properties. Table name (-t), column name (-c) and search query (-s) have to be provided." << std::endl;
				return 1;
			}

			if (interactive)
			{
				std::string user_in;
				while (true)
				{
					std::cout << "Waiting for input" << std::endl;
					getline(std::cin, user_in);

					if (user_in == std::string("-x"))
						break;
					ff_search(table_name, column_name, user_in, limit, and_op, fuzzy, offset);
				}
			}
			else
			{
				ff_search(table_name, column_name, search_query, limit, and_op, fuzzy, offset);
			}
			ff_search_end();
		}
		else
		{
			std::cout << "No mode provided\n";
		}
	}
	else
	{
		std::cout << "Not enought arguments provided\n";
	}

	// std::cout << "Done" << std::endl;
	return 0;
}