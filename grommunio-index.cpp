/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * SPDX-FileCopyrightText: 2022-2023 grommunio GmbH
 */
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <getopt.h>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mysql.h>
#include <optional>
#include <sqlite3.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>
#include <exmdbpp/constants.h>
#include <exmdbpp/queries.h>
#include <exmdbpp/requests.h>
#include <exmdbpp/util.h>
#include <libHX/proc.h>
#include <libHX/string.h>
#include <libxml/HTMLparser.h>
#include <sys/stat.h>

using namespace std::string_literals;
using namespace exmdbpp;
using namespace exmdbpp::constants;
using namespace exmdbpp::queries;
namespace fs = std::filesystem;

namespace {

using DB_ROW = char **;

class DB_RESULT { /* from gromox/database_mysql.hpp */
	public:
	DB_RESULT() = default;
	DB_RESULT(MYSQL_RES *r) noexcept : m_res(r) {}
	DB_RESULT(DB_RESULT &&o) noexcept : m_res(o.m_res) { o.m_res = nullptr; }
	~DB_RESULT() { clear(); }

	DB_RESULT &operator=(DB_RESULT &&o) noexcept
	{
		clear();
		m_res = o.m_res;
		o.m_res = nullptr;
		return *this;
	}
	void clear() {
		if (m_res != nullptr)
			mysql_free_result(m_res);
		m_res = nullptr;
	}
	operator bool() const noexcept { return m_res != nullptr; }
	bool operator==(std::nullptr_t) const noexcept { return m_res == nullptr; }
	bool operator!=(std::nullptr_t) const noexcept { return m_res != nullptr; }
	MYSQL_RES *get() const noexcept { return m_res; }
	void *release() noexcept
	{
		void *p = m_res;
		m_res = nullptr;
		return p;
	}

	size_t num_rows() const { return mysql_num_rows(m_res); }
	DB_ROW fetch_row() { return mysql_fetch_row(m_res); }

	private:
	MYSQL_RES *m_res = nullptr;
};

struct our_del {
	inline void operator()(FILE *x) const { fclose(x); }
	inline void operator()(MYSQL *x) const { mysql_close(x); }
	inline void operator()(xmlDoc *d) const { xmlFreeDoc(d); }
};

struct user_row {
	std::string username, dir, host;
};

}

using kvpairs = std::map<std::string, std::string>;

enum {RESULT_OK, RESULT_ARGERR_SYN, RESULT_ARGERR_SEM, RESULT_EXMDB_ERR}; ///< Exit codes
enum LEVEL {FATAL, ERROR, WARNING, STATUS, INFO, DEBUG, TRACE, LOGLEVELS}; ///< Log levels
static const char* levelname[] = {"FATAL", "ERROR", "WARNING", "STATUS", "INFO", "DEBUG", "TRACE"}; ///< Log level names

static int verbosity = STATUS; ///< Effective log level


/**
 * @brief      Logger struct
 *
 * Helper struct to allow template specialization for specific levels.
 *
 * @tparam     level  Level of the message to log
 */
template<LEVEL level>
struct msg
{
	/**
	 * @brief      Log a message
	 *
	 * Writes all arguments, without delimiter, to std::cout and automatically
	 * appends a newline.
	 *
	 * @param      args  Arguments to print
	 *
	 * @tparam     Args  Type of arguments to print
	 */
	template<typename... Args>
	explicit inline msg(const Args&... args)
	{
		if(level > verbosity)
			return;
		std::cout << "[" << levelname[level] << "] ";
		(std::cout << ... << args) << std::endl;
	}
};

#ifndef ENABLE_TRACE
/**
 * @brief     Template specialization to completely disable TRACE loglevel
 */
template<> struct msg<TRACE>
{template<typename... Args> explicit inline msg(const Args&...) {}};

#endif //!ENABLE_TRACE

/**
 * @brief      Create map by moving objects into it
 *
 * @param      first     Iterator to the first object ot map
 * @param      last      Iterator to the end of the input container
 * @param      key       Mapping function deriving the key from an object
 * @param      umap      The map to place the objects in
 *
 * @tparam     K         Key type
 * @tparam     V         Mapped type
 * @tparam     InputIt   Iterator type for input
 * @tparam     F         Funciton mapping V& -> K
 *
 * @returns    unordered_map containing objects
 */
template<typename K, typename V, class InputIt, typename F>
inline void mkMapMv(InputIt first, InputIt last, std::unordered_map<K, V>& umap, F&& key)
{
	umap.clear();
	umap.reserve(distance(first, last));
	for(; first != last; ++first)
		umap.try_emplace(key(*first), std::move(*first));
}

/**
 * @brief      Join strings from collection
 *
 * Join strings from a collection, optionally inserting another string between.
 *
 * A transform function can be used to transform an iterator to string.
 * The default transform dereferences the iterator.
 *
 * @param      first    Iterator to the beginning
 * @param      last     Iterator to the end
 * @param      glue     String to insert between elements
 * @param      tf       Input value transformation.
 *
 * @tparam     InputIt  Input iterator type
 * @tparam     F        Unary function transforming InputIt to string
 *
 * @return     Joined string
 */
template<class InputIt, typename F>
inline std::string strjoin(InputIt first, InputIt last, const std::string_view& glue = "",F&& tf=[](InputIt it){return *it;})
{
    if(first == last)
		return std::string();
	std::string str(tf(first));
	while(++first != last)
	{
		str += glue;
		str += tf(first);
	}
	return str;
}

/**
 * @brief      Join string from arguments
 *
 * @param      dest  String to join into
 * @param      args  Arguments to join to string
 *
 * @tparam     Argument types to join. operator += must be defined.
 *
 * @return     Reference to dest
 */
template<typename... Args>
inline std::string& strjoin(std::string& dest, Args&&... args)
{
	((dest += std::forward<Args>(args)), ...);
	return dest;
}

/**
 * @brief      Helper function mapping TaggedProval to tag ID
 */
inline uint32_t tagMapper(const structures::TaggedPropval& tp)
{return tp.tag;}

/**
 * @brief      Helper function to check for tagID
 *
 * @param      tp    TaggedPropval to check
 *
 * @tparam     tag   Tag ID to check for
 *
 * @return     true if tag ID matches, false otherwise
 */
template<uint32_t tag>
inline bool tagFinder(const structures::TaggedPropval& tp)
{return tp.tag == tag;}

/**
 * @brief      Adds a tag value .
 *
 * @param      dest  The destination
 * @param      pl    Property list to search
 *
 * @tparam     tag   Tag to search for
 */
template<uint32_t tag>
inline void addTagStrLine(std::string& dest, const ExmdbQueries::PropvalList& pl)
{
	static_assert(PropvalType::tagType(tag) != PropvalType::STRING || PropvalType::tagType(tag) != PropvalType::WSTRING,
	              "Can only add string tags");
	auto it = find_if(pl.begin(), pl.end(), tagFinder<tag>);
	if(it == pl.end())
		return;
	if(!dest.empty())
		dest += "\n";
	dest += it->value.str;
}

/**
 * @brief      Extract text from HTML document
 *
 * @param      body    String to append text to
 * @param      node    Parent XML node to traverse
 */
static void extractHtmlText(std::string &body, const xmlNode *node)
{
	if(!node)
		return;
	for(auto child = node->children; child; child = child->next) {
		if(child->type == XML_TEXT_NODE) {
			body += ' ';
			body += reinterpret_cast<const char*>(child->content);
		}
		extractHtmlText(body, child);
	}
}

/**
 * @brief      Append text (without tags) from HTML document
 *
 * @param      body    String to append to
 * @param      data    HTML document data
 * @param      len     HTML document length
 */
static void appendSanitizedHtml(std::string& body, const void* data, uint32_t len)
{
	std::unique_ptr<xmlDoc, our_del> doc(htmlReadMemory(static_cast<const char*>(data), len, nullptr, "utf-8",
	                                                    HTML_PARSE_NOERROR | HTML_PARSE_NOWARNING | HTML_PARSE_NONET));
	if(!doc) {
		msg<WARNING>("failed to parse HTML data");
		return;
	}
	extractHtmlText(body, xmlDocGetRootElement(doc.get()));
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * @brief      Index database management class
 */
class IndexDB
{
public:
	IndexDB() = default;

	IndexDB& operator=(IndexDB&& other)
	{
		if(db)
			sqlite3_close(db);
		recheck = other.recheck;
		dbpath = std::move(other.dbpath);
		usrpath	= std::move(other.usrpath);
		client = std::move(other.client);
		reuse = std::move(other.reuse);
		namedProptags = std::move(other.namedProptags);
		db = other.db;
		update = other.update;
		other.db = nullptr;
		return *this;
	}

	~IndexDB()
	{
		if(db)
			sqlite3_close(db);
	}

	/**
	 * @brief      Open an index databse
	 *
	 * Create the index sqlite database and connect to the exmdb server.
	 *
	 * If no outpath is specified, the default location is
	 * `exmdb/index.sqlite3` within the userdir.
	 *
	 * @param      userdir    Path to the user's home-directory
	 * @param      exmdbHost  Host name for exmdb connection
	 * @param      exmdbPort  Port for exmdb connection
	 * @param      outpath    Path of the output database or empty for default
	 */
	IndexDB(const fs::path& userdir, const std::string& exmdbHost, const std::string& exmdbPort, const std::string& outpath,
	        bool create=false, bool recheck=false) :
	    usrpath(userdir), client(exmdbHost, exmdbPort, userdir, true, ExmdbClient::AUTO_RECONNECT),
	    recheck(recheck)
	{
		if(outpath.empty())
		{
			dbpath = userdir;
			dbpath /= "exmdb";
			if(!fs::exists(dbpath))
				throw std::runtime_error("Cannot access "s + dbpath.c_str() + " (absent or permission problem)");
			dbpath /= "index.sqlite3";
		}
		else
		{
			dbpath = outpath;
			if(fs::is_directory(dbpath))
				dbpath /= "index.sqlite3";
		}
		update = fs::exists(dbpath);
		if (update)
			msg<STATUS>("Updating existing index "s + dbpath.c_str());
		else
			msg<STATUS>("Creating new index "s + dbpath.c_str());
		int res = sqlite3_open(dbpath.c_str(), &db);
		if(res != SQLITE_OK)
			throw std::runtime_error("Failed to open index database: "s + sqlite3_errmsg(db));
		chmod(dbpath.c_str(), 0660); /* sqlite3_open ignores umask */
		if(update && create)
		{
			sqliteExec("DROP TABLE IF EXISTS hierarchy;"
			           "DROP TABLE IF EXISTS messages");
			update = false;
		}
		res = sqliteExec("CREATE TABLE IF NOT EXISTS hierarchy ("
		                 " folder_id INTEGER PRIMARY KEY,"
		                 " commit_max INTEGER NOT NULL,"
		                 " max_cn INTEGER NOT NULL);\n"
		                 "CREATE VIRTUAL TABLE IF NOT EXISTS messages USING fts5 ("
		                 " sender, sending, recipients, "
		                 " subject, content, attachments,"
		                 " others, message_id,"
		                 " attach_indexed UNINDEXED,"
		                 " entryid UNINDEXED,"
		                 " change_num UNINDEXED,"
		                 " folder_id UNINDEXED,"
		                 " message_class UNINDEXED,"
		                 " date UNINDEXED, "
		                 " tokenize=trigram)");
		if(res != SQLITE_OK)
			throw std::runtime_error("Failed to initialize index database: "s + sqlite3_errmsg(db));
	}

	/**
	 * @brief      Refresh the index
	 *
	 * Checks all generic folders for changes.
	 * Checks all messages in out-of-date folders for changes.
	 * Deletes modified messages.
	 * Inserts new/modified messages-
	 * Updates hierarchy.
	 */
	void refresh()
	{
		auto [messages, hierarchy] = getUpdates();
		if(messages.size() == 0 && hierarchy.size() == 0)
		{
			msg<STATUS>("Index is up-to-date.");
			return;
		}
		msg<STATUS>("Updating index...");
		removeMessages(messages);
		insertMessages(messages);
		refreshHierarchy(hierarchy);
		msg<STATUS>("Index updated.");
	}

private:
	using PropvalMap = std::unordered_map<uint32_t, structures::TaggedPropval>; ///< Tag ID -> TaggedPropval mapping

	/**
	 * @brief      SQLite3 statement wrapper class
	 *
	 * Implements some convenience function for raw sqlite3 statements
	 */
	struct SQLiteStmt
	{
		/**
		 * @brief      Create statement from query string
		 *
		 * @param      db    Database object
		 * @param      zSql  Query string
		 *
		 * @throws     runtime_error	Statement compilation failed
		 */
		SQLiteStmt(sqlite3* db, const char* zSql)
		{
			int res = sqlite3_prepare_v2(db, zSql, -1, &stmt, nullptr);
			if(res != SQLITE_OK)
				throw std::runtime_error(sqlite3_errmsg(db));
		}

		~SQLiteStmt() {sqlite3_finalize(stmt);}

		/**
		 * @brief      Convert to raw sqlite3_stmt
		 */
		operator sqlite3_stmt*() {return stmt;}

		/**
		 * @brief      Call sqlite3 function on statement
		 *
		 * Statement argument is supplied automatically.
		 *
		 * @param      func   Function to call
		 * @param      args   Arguments to forward to function
		 *
		 * @tparam     fArgs  Arguments of the function
		 * @tparam     cArgs  Arguments forwarded to function
		 *
		 * @throws     runtime_error      Function did not return SQLITE_OK
		 */
		template<typename... fArgs, typename... cArgs>
		void call(int(*func)(sqlite3_stmt*, fArgs...), cArgs&&... args)
		{
			int res = func(stmt, std::forward<cArgs>(args)...);
			if(res != SQLITE_OK)
				throw std::runtime_error(sqlite3_errmsg(sqlite3_db_handle(stmt)));
		}

		/**
		 * @brief      Bind non-empty string to statement
		 *
		 * If supplied string is empty, not bind is issued.
		 *
		 * @param      index  Parameter index to bind
		 * @param      str    String to bind
		 *
		 * @throws     out_of_range       String is too long to bind
		 */
		void bindText(int index, const std::string_view& str)
		{
			if(str.size() == 0)
				return call(sqlite3_bind_null, index);
			if(str.size() > std::numeric_limits<int>::max())
				throw std::out_of_range("String lengths exceeds maximum");
			call(sqlite3_bind_text, index, str.data(), int(str.size()), SQLITE_STATIC);
		}

		/**
		 * @brief      Bind non-empty string to statement.
		 *
		 * Resolves parameter name and calls bindText(int, const string&).
		 *
		 * @param      name  Parameter name to bind
		 * @param      str   String to bind
		 */
		void bindText(const char* name, const std::string_view& str)
		{bindText(indexOf(name), str);}

		/**
		 * @brief      Find index of named bind parameter
		 *
		 * @param      name  Name of the parameter
		 *
		 * @return     Index of named parameter
		 *
		 * @throws     out_of_range       Named parameter not found
		 */
		int indexOf(const char* name)
		{
			int index = sqlite3_bind_parameter_index(stmt, name);
			if(!index)
				throw std::out_of_range("Cannot find named bind parameter "s + name);
			return index;
		}

		/**
		 * @brief      Execute statement
		 *
		 * Successful if result is either SQLITE_DONE or SQLITE_ROW.
		 *
		 * @return     Return code of sqlite3_step
		 *
		 * @throws     runtime_error      Execution was not successful
		 */
		int exec()
		{
			int result = sqlite3_step(stmt);
			if(result != SQLITE_DONE && result != SQLITE_ROW)
				throw std::runtime_error("SQLite query failed: " + std::to_string(result));
			return result;
		}

		sqlite3_stmt* stmt; ///< Managed statement
	};

	/**
	 * @brief      Helper struct for messages
	 */
	struct Message
	{
		inline Message(uint64_t mid, uint64_t fid,  structures::TaggedPropval& entryid) : mid(mid), fid(fid), entryid(std::move(entryid)) {}

		uint64_t mid, fid;
		structures::TaggedPropval entryid;
	};

	/**
	 * @brief      Helper struct for hierarchy information
	 */
	struct Hierarchy
	{
		Hierarchy(uint64_t folderId, uint64_t lctm, uint64_t maxCn) : folderId(folderId), lctm(lctm), maxCn(maxCn) {}
		uint64_t folderId, lctm, maxCn;
	};

	struct
	{
		std::string attchs, body, other, rcpts, sender, sending, subject, messageclass;
		PropvalMap props;

		void reset()
		{
			rcpts.clear(); attchs.clear(); sending.clear(); sender.clear(); body.clear();
			props.clear(); other.clear();
		}
	} reuse; ///< Objects that can be reused to save on memory allocations

	static std::array<structures::PropertyName, 14> namedTags; ///< Array of named tags to query
	static constexpr std::array<uint16_t, 14> namedTagTypes = {
	    PropvalType::STRING_ARRAY,
	    PropvalType::STRING, PropvalType::STRING, PropvalType::STRING,
	    PropvalType::STRING, PropvalType::STRING, PropvalType::STRING,
	    PropvalType::STRING, PropvalType::STRING,
	    PropvalType::STRING, PropvalType::STRING,
	    PropvalType::STRING, PropvalType::STRING,
	    PropvalType::STRING_ARRAY
	}; ///< Types of the named tags

	static constexpr std::array<uint32_t, 15> msgtags1 = {
	     PropTag::ENTRYID, PropTag::SENTREPRESENTINGNAME, PropTag::SENTREPRESENTINGSMTPADDRESS,
	     PropTag::SENTREPRESENTINGEMAILADDRESS, PropTag::SENDEREMAILADDRESS,
	     PropTag::SUBJECT, PropTag::BODY, PropTag::SENDERNAME,
	     PropTag::SENDERSMTPADDRESS, PropTag::INTERNETCODEPAGE,
	     PropTag::CHANGENUMBER, PropTag::MESSAGECLASS,
	     PropTag::MESSAGEDELIVERYTIME, PropTag::LASTMODIFICATIONTIME,
	     PropTag::HTML,
	}; ///< Part 1 of message tags to query

	static constexpr std::array<uint32_t, 21> msgtags2 = {
	    PropTag::DISPLAYNAME, PropTag::DISPLAYNAMEPREFIX, PropTag::HOMETELEPHONENUMBER,
	    PropTag::MOBILETELEPHONENUMBER, PropTag::BUSINESSTELEPHONENUMBER,
	    PropTag::BUSINESSFAXNUMBER, PropTag::ASSISTANTTELEPHONENUMBER,
	    PropTag::BUSINESS2TELEPHONENUMBER, PropTag::CALLBACKTELEPHONENUMBER,
	    PropTag::CARTELEPHONENUMBER, PropTag::COMPANYMAINTELEPHONENUMBER,
	    PropTag::HOME2TELEPHONENUMBER, PropTag::HOMEFAXNUMBER, PropTag::OTHERTELEPHONENUMBER,
	    PropTag::PAGERTELEPHONENUMBER, PropTag::PRIMARYFAXNUMBER,
	    PropTag::PRIMARYTELEPHONENUMBER, PropTag::RADIOTELEPHONENUMBER, PropTag::TELEXNUMBER,
	    PropTag::COMPANYNAME, PropTag::TITLE,
	}; ///< Part 2 of message tags to query

	fs::path usrpath; ///< Path to the user's home directory
	fs::path dbpath; ///< Path to the index database
	ExmdbClient client; ///< Exmdb client to use
	std::vector<uint32_t> namedProptags; ///< Store specific named proptag IDs
	sqlite3* db = nullptr; ///< SQLite database connection
	bool update = false; ///< Whether index is updated
	bool recheck = false; ///< Whether to check all folders regardless of timestamp

	/**
	 * @brief      Convenience wrapper for sqlite3_index
	 *
	 * Logs a warning if the execution was not successful.
	 *
	 * @param      query  Query string to execute
	 *
	 * @return     Result code of sqlite3_exec
	 */
	inline int sqliteExec(const char* query)
	{
		int res	= sqlite3_exec(db, query, nullptr, nullptr, nullptr);
		if(res != SQLITE_OK && res != SQLITE_ROW && res != SQLITE_DONE)
			msg<WARNING>("SQLite query failed: ", res, '(', sqlite3_errmsg(db), ')');
		return res;
	}

	/**
	 * @brief      Find tag in list
	 *
	 * @param      tplist  List of tagged properties
	 * @param      tag     Tag ID to find
	 *
	 * @return     Reference to the tag
	 *
	 * @throws     out_of_range       Tag was not found
	 */
	structures::TaggedPropval& getTag(ExmdbQueries::PropvalList& tplist, uint32_t tag)
	{
		auto res = find_if(tplist.begin(), tplist.end(), [tag](const structures::TaggedPropval& t){return t.tag == tag;});
		if(res == tplist.end()) {
			char temp[128];
			snprintf(temp, std::size(temp), "failed to find required tag 0x%08X - cannot proceed", tag);
			throw std::out_of_range(temp);
		}
		return *res;
	}

	/**
	 * @brief      Check folders and messages for updates
	 *
	 * @return     Pair of messages and hierarchy entries to update
	 */
	std::pair<std::vector<Message>, std::vector<Hierarchy>> getUpdates()
	{
		using namespace exmdbpp::constants;
		using namespace exmdbpp::requests;
		using namespace exmdbpp::structures;
		static const uint32_t fTags[] = {PropTag::FOLDERID, PropTag::LOCALCOMMITTIMEMAX};
		static const uint32_t cTags[] = {PropTag::MID, PropTag::CHANGENUMBER, PropTag::ENTRYID};
		static const uint64_t ipmsubtree = util::makeEidEx(1, PrivateFid::IPMSUBTREE);
		static const Restriction genericOnly = Restriction::PROPERTY(Restriction::EQ, 0,
		                                                             TaggedPropval(PropTag::FOLDERTYPE, uint32_t(1)));
		msg<STATUS>("Checking for updates...");
		auto lhtResponse = client.send<LoadHierarchyTableRequest>(usrpath, ipmsubtree, "", TableFlags::DEPTH);
		auto qtResponse = client.send<QueryTableRequest>(usrpath, "", 0, lhtResponse.tableId, fTags, 0, lhtResponse.rowCount);
		client.send<UnloadTableRequest>(usrpath, lhtResponse.tableId);
		msg<DEBUG>("Loaded ", qtResponse.entries.size(), " folders");
		SQLiteStmt stmt(db, "SELECT commit_max, max_cn FROM hierarchy WHERE folder_id=?");
		std::vector<Message> messages;
		std::vector<Hierarchy> hierarchy;
		for(auto& entry : qtResponse.entries) try
		{
			uint64_t lastCn = 0, maxCn = 0;
			uint64_t folderIdGc = getTag(entry, PropTag::FOLDERID).value.u64;
			uint64_t folderId = util::gcToValue(folderIdGc);
			uint64_t lctm = util::nxTime(getTag(entry, PropTag::LOCALCOMMITTIMEMAX).value.u64);
			if(update)
			{
				stmt.call(sqlite3_reset);
				stmt.call(sqlite3_bind_int64, 1, folderId);
				int res = stmt.exec();
				if(res == SQLITE_ROW)
				{
					if(uint64_t(sqlite3_column_int64(stmt, 0)) == lctm && !recheck)
					{
						msg<TRACE>("Folder ", folderId, " is up to date");
						continue;
					}
					lastCn = maxCn = sqlite3_column_int64(stmt, 1);
				}
			}
			auto lctResponse = client.send<LoadContentTableRequest>(usrpath, 0, folderIdGc, "", 0);
			auto contents = client.send<QueryTableRequest>(usrpath, "", 0, lctResponse.tableId, cTags, 0, lctResponse.rowCount);
			client.send<UnloadTableRequest>(usrpath, lctResponse.tableId);
			for(auto& content : contents.entries)
			{
				uint64_t cn = util::gcToValue(getTag(content, PropTag::CHANGENUMBER).value.u64);
				if(cn <= lastCn)
					continue;
				maxCn = std::max(maxCn, cn);
				messages.emplace_back(getTag(content, PropTag::MID).value.u64, folderId, getTag(content, PropTag::ENTRYID));
			}
			msg<TRACE>("Checked folder ", folderId, " with ", contents.entries.size(), " messages. ",
			           "Total updates now at ", messages.size(), ".");
			hierarchy.emplace_back(folderId, lctm, maxCn);
		} catch (const std::out_of_range &e) {
			msg<ERROR>(e.what());
			throw EXIT_FAILURE;
		}
		msg<INFO>("Need to update ", messages.size(), " message", messages.size() == 1? "": "s",
		           " and ", hierarchy.size(), " hierarchy entr", hierarchy.size() == 1? "y" : "ies", '.');
		return {messages, hierarchy};
	}

	/**
	 * @brief      Remove any messages that need updates
	 *
	 * @param      messages  Messages to remove
	 */
	void removeMessages(const std::vector<Message>& messages)
	{
		msg<DEBUG>("Removing ", messages.size(), " modified messages");
		if(!update)
			return;
		SQLiteStmt stmt(db, "DELETE FROM messages WHERE message_id=?");
		sqliteExec("BEGIN");
		for(const auto& message : messages)
		{
			stmt.call(sqlite3_reset);
			stmt.call(sqlite3_bind_int64, 1, int64_t(message.mid));
			stmt.exec();
		}
		sqliteExec("COMMIT");
	}

	/**
	 * @brief      Insert new/updated messages
	 *
	 * @param      messages  Messages to insert
	 */
	void insertMessages(const std::vector<Message>& messages)
	{
		msg<DEBUG>("Inserting new messages");
		if(messages.empty())
			return;
		client.reconnect();
		namedProptags = getNamedProptags();
		std::vector<uint32_t> msgtags;
		msgtags.resize(namedProptags.size()+msgtags1.size()+msgtags2.size());
		auto tagend = copy(namedProptags.begin(), namedProptags.end(), msgtags.begin());
		tagend = copy(msgtags1.begin(), msgtags1.end(), tagend);
		copy(msgtags2.begin(), msgtags2.end(), tagend);
		SQLiteStmt stmt(db, "INSERT INTO messages (sender, sending, recipients, subject, "
		                    "content, attachments, others, message_id, attach_indexed, entryid, change_num, folder_id,"
		                    " message_class, date) VALUES (:sender, :sending, :recipients, :subject, :content, "
		                    ":attachments, :others, :message_id, :attach_indexed, :entryid, :change_num, :folder_id, "
		                    ":message_class, :date)");
		sqliteExec("BEGIN");
		for(const Message& message : messages)
		{
			try {insertMessage(stmt, message, msgtags);}
			catch (const std::exception& e)
			{msg<ERROR>("Failed to insert message ", message.fid, "/", util::gcToValue(message.mid), ": ", e.what());}
		}
		sqliteExec("COMMIT");
	}

	/**
	 * @brief      Insert a new message into the index
	 *
	 * @param      stmt     Prepared insert statement
	 * @param      message  Message to insert
	 * @param      msgtags  List of tag IDs to query
	 */
	void insertMessage(SQLiteStmt& stmt, const Message& message, const std::vector<uint32_t>& msgtags)
	{
		using namespace constants;
		using namespace requests;
		static const uint32_t attchProps[] = {PropTag::ATTACHLONGFILENAME};
		msg<TRACE>("Inserting message ", message.fid, "/", util::gcToValue(message.mid));
		reuse.reset();
		stmt.call(sqlite3_reset);
		uint32_t instance = client.send<LoadMessageInstanceRequest>(usrpath, "", 65001, false, 0, message.mid).instanceId;
		auto rcpts = client.send<GetMessageInstanceRecipientsRequest>(usrpath, instance, 0, std::numeric_limits<uint16_t>::max());
		auto attchs = client.send<QueryMessageInstanceAttachmentsTableRequest>(usrpath, instance, attchProps, 0, 0);
		auto propvals = client.send<GetInstancePropertiesRequest>(usrpath, 0, instance, msgtags).propvals;
		client.send<UnloadInstanceRequest>(usrpath, instance);
		msg<TRACE>(" Received ", propvals.size(), "/", msgtags.size(), " properties");
		for(const ExmdbQueries::PropvalList& pl : rcpts.entries)
		{
			addTagStrLine<PropTag::DISPLAYNAME>(reuse.rcpts, pl);
			addTagStrLine<PropTag::SMTPADDRESS>(reuse.rcpts, pl);
		}
		for(const ExmdbQueries::PropvalList& pl : attchs.entries)
			if(pl.size() == 1 && pl[0].tag == PropTag::ATTACHLONGFILENAME)
				reuse.attchs += pl[0].value.str;

		mkMapMv(propvals.begin(), propvals.end(), reuse.props, tagMapper);
		decltype(reuse.props)::iterator it;
		if((it = reuse.props.find(PropTag::SENTREPRESENTINGNAME)) != reuse.props.end())
			reuse.sending += it->second.value.str;
		if((it = reuse.props.find(PropTag::SENTREPRESENTINGSMTPADDRESS)) != reuse.props.end())
			strjoin(reuse.sending, "\n", it->second.value.str);
		if((it = reuse.props.find(PropTag::SENTREPRESENTINGEMAILADDRESS)) != reuse.props.end())
			strjoin(reuse.sending, "\n", it->second.value.str);
		if((it = reuse.props.find(PropTag::SENDERNAME)) != reuse.props.end())
			reuse.sender += it->second.value.str;
		if((it = reuse.props.find(PropTag::SENDERSMTPADDRESS)) != reuse.props.end())
			strjoin(reuse.sender, "\n", it->second.value.str);
		if((it = reuse.props.find(PropTag::SENDEREMAILADDRESS)) != reuse.props.end())
			strjoin(reuse.sender, "\n", it->second.value.str);
		if((it = reuse.props.find(PropTag::BODY)) != reuse.props.end())
			reuse.body = it->second.value.str;
		if((it = reuse.props.find(PropTag::HTML)) != reuse.props.end())
			appendSanitizedHtml(reuse.body, it->second.binaryData(), it->second.binaryLength());
		if((it = reuse.props.find(PropTag::MESSAGECLASS)) != reuse.props.end())
			reuse.messageclass = it->second.value.str;
		if((it = reuse.props.find(PropTag::COMPANYNAME)) != reuse.props.end())
			strjoin(reuse.other, "\n", it->second.value.str);
		if((it = reuse.props.find(PropTag::TITLE)) != reuse.props.end())
			strjoin(reuse.other, "\n", it->second.value.str);
		for(const auto& entry : namedProptags)
		{
			if((it = reuse.props.find(entry)) == reuse.props.end())
				continue;
			structures::TaggedPropval& tp = it->second;
			if(tp.type != PropvalType::STRING && tp.type != PropvalType::STRING_ARRAY)
				continue;
			reuse.other += reuse.other.empty()? "" : "\n";
			if(tp.type == PropvalType::STRING)
				reuse.other += tp.value.str;
			else
				reuse.other += strjoin(tp.value.astr.begin(), tp.value.astr.end(), "\n", [](char** it){return *it;});
		}
		stmt.bindText(":sender", reuse.sender);
		stmt.bindText(":sending", reuse.sending);
		stmt.bindText(":recipients", reuse.rcpts);
		stmt.bindText(":subject", (it = reuse.props.find(PropTag::SUBJECT)) != reuse.props.end()? it->second.value.str : "");
		stmt.bindText(":content", reuse.body);
		stmt.bindText(":attachments", reuse.attchs);
		stmt.bindText(":others", reuse.other);
		stmt.call(sqlite3_bind_int64, stmt.indexOf(":message_id"), util::gcToValue(message.mid));
		stmt.call(sqlite3_bind_int,	stmt.indexOf(":attach_indexed"), int(reuse.attchs.length() > 0));
		stmt.call(sqlite3_bind_blob64, stmt.indexOf(":entryid"), message.entryid.binaryData(), message.entryid.binaryLength(), nullptr);
		stmt.call(sqlite3_bind_int64, stmt.indexOf(":folder_id"), message.fid);
		if((it = reuse.props.find(PropTag::CHANGENUMBER)) != reuse.props.end())
			stmt.call(sqlite3_bind_int64, stmt.indexOf(":change_num"), util::gcToValue(it->second.value.u64));
		stmt.bindText(":message_class", reuse.messageclass);
		if((it = reuse.props.find(PropTag::LASTMODIFICATIONTIME)) != reuse.props.end() ||
		   (it = reuse.props.find(PropTag::MESSAGEDELIVERYTIME)) != reuse.props.end())
			stmt.call(sqlite3_bind_int64, stmt.indexOf(":date"), util::nxTime(it->second.value.u64));
		stmt.exec();
	}

	/**
	 * @brief      Retrieve IDs of named properties
	 *
	 * @return     Named property IDs
	 */
	std::vector<uint32_t> getNamedProptags()
	{
		using namespace requests;
		auto response = client.send<GetNamedPropIdsRequest>(usrpath, false, namedTags);
		if(response.propIds.size() != namedTagTypes.size())
			throw std::out_of_range("Number of named property IDs does not match expected count");
		std::vector<uint32_t> propTags(namedTagTypes.size());
		transform(namedTagTypes.begin(), namedTagTypes.end(), response.propIds.begin(), propTags.begin(),
		          [](uint16_t id, uint16_t type) {return uint32_t(id) << 16 | type;});
		return propTags;
	}

	/**
	 * @brief      Refresh the folder hierarchy
	 *
	 * @param      hierarchy  List of hierarchy entries
	 */
	void refreshHierarchy(const std::vector<Hierarchy>& hierarchy)
	{
		SQLiteStmt stmt(db, "REPLACE INTO hierarchy (folder_id, commit_max, max_cn) VALUES (?, ?, ?)");
		sqliteExec("BEGIN");
		for(const Hierarchy& h : hierarchy)
		{
			stmt.call(sqlite3_reset);
			stmt.call(sqlite3_bind_int64, 1, h.folderId);
			stmt.call(sqlite3_bind_int64, 2, h.lctm);
			stmt.call(sqlite3_bind_int64, 3, h.maxCn);
			stmt.exec();
		}
		sqliteExec("COMMIT");
	}
};

std::array<structures::PropertyName, 14> IndexDB::namedTags = {
    structures::PropertyName(structures::GUID("00020329-0000-0000-C000-000000000046"), "Keywords"), //categories
    structures::PropertyName(structures::GUID("00062004-0000-0000-C000-000000000046"), 0x8005),     //fileas
    structures::PropertyName(structures::GUID("00062002-0000-0000-C000-000000000046"), 0x8208),     //location
    structures::PropertyName(structures::GUID("00062004-0000-0000-C000-000000000046"), 0x8083),     //email1
    structures::PropertyName(structures::GUID("00062004-0000-0000-C000-000000000046"), 0x8080),     //email1_name
    structures::PropertyName(structures::GUID("00062004-0000-0000-C000-000000000046"), 0x8093),     //email2
    structures::PropertyName(structures::GUID("00062004-0000-0000-C000-000000000046"), 0x8090),     //email2_name
    structures::PropertyName(structures::GUID("00062004-0000-0000-C000-000000000046"), 0x80a3),     //email3
    structures::PropertyName(structures::GUID("00062004-0000-0000-C000-000000000046"), 0x80a0),     //email3_name
    structures::PropertyName(structures::GUID("00062004-0000-0000-C000-000000000046"), 0x801a),     //home_address
    structures::PropertyName(structures::GUID("00062004-0000-0000-C000-000000000046"), 0x801c),     //other_address
    structures::PropertyName(structures::GUID("00062004-0000-0000-C000-000000000046"), 0x801b),     //work_address
    structures::PropertyName(structures::GUID("00062003-0000-0000-C000-000000000046"), 0x811f),     //task_owner
    structures::PropertyName(structures::GUID("00062008-0000-0000-C000-000000000046"), 0x8539)      //companies
};

///////////////////////////////////////////////////////////////////////////////////////////////////

static std::string exmdbHost; ///< Exmdb host to connect to
static std::string exmdbPort; ///< Port of the exmdb connection
static std::optional<std::string> userpath; ///< Path to the user's home directory
static std::string outpath; ///< Index database path (empty for default)
static bool recheck = false; ///< Check folders even when they were not changed since the last indexing
static bool create = false; ///< Always create a new index instead of updating
static bool do_all_users;

/**
 * @brief      Print help message
 *
 * @param      name   Name of the program
 */
[[noreturn]] static void printHelp(const char* name)
{
	std::cout << "grommunio mailbox indexing tool\n"
	        "\nUsage: " << name << " [-c] [-e host] [-f] [-h] [-o file] [-p port] [-q] [-v] <userpath>\n"
	          "Usage: " << name << " -A [-c] [-f] [-h] [-p port] [-q] [-v]\n"
	        "\nPositional arguments:\n"
	        "\t userpath\t\tPath to the user's mailbox directory\n"
	        "\nOptional arguments:\n"
	        "\t-A\t--all    \tAutomatically process all local users (-e, -o ignored)\n"
	        "\t-c\t--create \tCreate a new index instead of updating\n"
	        "\t-e\t--host   \tHostname of the exmdb server\n"
	        "\t-h\t--help   \tShow this help message and exit\n"
	        "\t-o\t--out    \tWrite index database to specific file\n"
	        "\t-p\t--port   \tPort of the exmdb server\n"
	        "\t-q\t--quiet  \tReduce verbosity level\n"
	        "\t-r\t--recheck\tCheck all messages regardless of folder timestamp\n"
	        "\t-v\t--verbose\tIncrease verbosity level\n";
	exit(0);
}

/**
 * @brief      Parse command line arguments
 *
 * Does not return in case of error.
 *
 * @param      argv  nullptr-terminated array of command line arguments
 */
static void parseArgs(int argc, char **argv)
{
	static const struct option longopts[] = {
		{"all", false, nullptr, 'A'},
		{"create", false, nullptr, 'c'},
		{"host", true, nullptr, 'e'},
		{"help", false, nullptr, 'h'},
		{"outpath", true, nullptr, 'o'},
		{"port", true, nullptr, 'p'},
		{"quiet", false, nullptr, 'q'},
		{"recheck", false, nullptr, 'r'},
		{"verbose", false, nullptr, 'v'},
		{},
	};

	int c;
	while ((c = getopt_long(argc, argv, "Ace:ho:p:qrv", longopts, nullptr)) >= 0) {
		switch (c) {
		case 'A': do_all_users = true; break;
		case 'c': create = true; break;
		case 'e': exmdbHost = optarg; break;
		case 'h': printHelp(*argv); break;
		case 'o': outpath = optarg; break;
		case 'p': exmdbPort = optarg; break;
		case 'q': --verbosity; break;
		case 'r': recheck = true; break;
		case 'v': ++verbosity; break;
		default:
			exit(RESULT_ARGERR_SYN);
		}
	}
	if (do_all_users) {
		if (!exmdbHost.empty() || !outpath.empty() || argc > optind) {
			msg<FATAL>("Cannot combine -A with -e/-o/userpath");
			exit(RESULT_ARGERR_SYN);
		}
	} else {
		if (argc > optind)
			userpath.emplace(argv[optind++]);
		if (!userpath.has_value()) {
			msg<FATAL>("Usage: grommunio-index MAILDIR");
			msg<STATUS>("Option overview: grommunio-index -h");
			exit(RESULT_ARGERR_SYN);
		}
	}
	if(exmdbHost.empty())
		exmdbHost = "localhost";
	if(exmdbPort.empty())
		exmdbPort = "5000";
	verbosity = std::min(std::max(verbosity, 0), LOGLEVELS-1);
}

static int single_mode()
{
	msg<DEBUG>("exmdb=", exmdbHost, ":", exmdbPort, ", user=", userpath.value(), ", output=", outpath.empty()? "<default>" : outpath);
	IndexDB cache;
	try {
		cache = IndexDB(userpath.value(), exmdbHost, exmdbPort, outpath, create, recheck);
		cache.refresh();
	} catch(const std::runtime_error& err) {
		msg<FATAL>(err.what());
		return RESULT_ARGERR_SEM;
	} catch(int e) {
		return e;
	}
	return 0;
}

static kvpairs am_read_config(const char *path)
{
	std::unique_ptr<FILE, our_del> fp(fopen(path, "r"));
	if (fp == nullptr) {
		fprintf(stderr, "%s: %s\n", path, strerror(errno));
		throw EXIT_FAILURE;
	}
	kvpairs vars;
	hxmc_t *ln = nullptr;
	while (HX_getl(&ln, fp.get()) != nullptr) {
		auto eq = strchr(ln, '=');
		if (eq == nullptr)
			continue;
		if (*ln == '#')
			continue;
		HX_chomp(ln);
		*eq++ = '\0';
		HX_strrtrim(ln);
		HX_strltrim(eq);
		vars[ln] = eq;
	}
	HXmc_free(ln);
	/* fill with defaults if empty */
	vars.try_emplace("mysql_username", "root");
	vars.try_emplace("mysql_dbname", "grommunio");
	vars.try_emplace("mysql_host", "localhost");
	return vars;
}

static std::vector<user_row> am_read_users(kvpairs &&vars)
{
	std::unique_ptr<MYSQL, our_del> conn(mysql_init(nullptr));
	if (conn == nullptr)
		throw EXIT_FAILURE;
	auto pass = vars.find("mysql_password");
	if (mysql_real_connect(conn.get(), vars["mysql_host"].c_str(),
	    vars["mysql_username"].c_str(), pass != vars.end() ? pass->second.c_str() : nullptr,
	    vars["mysql_dbname"].c_str(), strtoul(vars["mysql_port"].c_str(), nullptr, 0),
	    nullptr, 0) == nullptr) {
		fprintf(stderr, "mysql_connect: %s\n", mysql_error(conn.get()));
		throw EXIT_FAILURE;
	}
	if (mysql_set_character_set(conn.get(), "utf8mb4") != 0) {
		fprintf(stderr, "\"utf8mb4\" not available: %s", mysql_error(conn.get()));
		throw EXIT_FAILURE;
	}

	static constexpr char query[] =
		"SELECT u.username, u.maildir, s.hostname FROM users u "
		"LEFT JOIN servers s ON u.homeserver=s.id "
		"WHERE u.maildir != ''";
	if (mysql_query(conn.get(), query) != 0) {
		fprintf(stderr, "%s: %s\n", query, mysql_error(conn.get()));
		throw EXIT_FAILURE;
	}
	DB_RESULT myres = mysql_store_result(conn.get());
	if (myres == nullptr) {
		fprintf(stderr, "result: %s\n", mysql_error(conn.get()));
		throw EXIT_FAILURE;
	}
	std::vector<user_row> ulist;
	for (DB_ROW row; (row = myres.fetch_row()) != nullptr; ) {
		if (row[0] == nullptr)
			continue;
		auto host = row[2] != nullptr && row[2][0] != '\0' ? row[2] : "::1";
		ulist.emplace_back(user_row{row[0], row[1], host});
	}
	return ulist;
}

int main(int argc, char **argv) try
{
	parseArgs(argc, argv);

	auto cfg = am_read_config("/etc/gromox/mysql_adaptor.cfg");
	/* Generated index files should not be world-readable */
	umask(07);
	if (!do_all_users)
		return single_mode();

	/*
	 * Regular users cannot call setgid(123) even if 123 is in the
	 * secondary group list already. The .service file has to ensure it has
	 * the right group already.
	 */
	if (geteuid() == 0) {
		auto ret = HXproc_switch_user("groindex", "groweb");
		if (static_cast<int>(ret) < 0) {
			fprintf(stderr, "switch_user grommunio/groweb: %s\n", strerror(errno));
			return EXIT_FAILURE;
		}
		/* setuid often disables coredumps, so restart to get them back. */
		execv(argv[0], argv);
	}
	int bigret = EXIT_SUCCESS;
	static const std::string index_root = "/var/lib/grommunio-web/sqlite-index";
	for (auto &&u : am_read_users(std::move(cfg))) {
		auto index_home = index_root + "/" + u.username;
		if (mkdir(index_home.c_str(), 0777) != 0 && errno != EEXIST) {
			fprintf(stderr, "mkdir %s: %s\n", index_home.c_str(), strerror(errno));
			bigret = EXIT_FAILURE;
			continue;
		}
		auto index_file = index_home + "/index.sqlite3";
		auto now = time(nullptr);
		char tmbuf[32];
		strftime(tmbuf, sizeof(tmbuf), "%FT%T", localtime(&now));
		fprintf(stderr, "[%s] %s %s -e %s -o %s\n", tmbuf, argv[0],
			u.dir.c_str(), u.host.c_str(), index_file.c_str());
		userpath.emplace(std::move(u.dir));
		exmdbHost = std::move(u.host);
		outpath = std::move(index_file);
		auto ret = single_mode();
		if (ret != 0) {
			fprintf(stderr, "\t... exited with status %d\n", ret);
			bigret = EXIT_FAILURE;
		}
		fprintf(stderr, "\n");
	}
	return bigret;
} catch (int e) {
	return e;
}
