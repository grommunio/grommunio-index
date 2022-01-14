/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * SPDX-FileCopyrightText: 2022 grommunio GmbH
 */
#include <algorithm>
#include <filesystem>
#include <iostream>

#include <exmdbpp/constants.h>
#include <exmdbpp/queries.h>
#include <exmdbpp/requests.h>
#include <exmdbpp/util.h>
#include <sqlite3.h>


using namespace std;
using namespace exmdbpp;
using namespace exmdbpp::constants;
using namespace exmdbpp::queries;

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
		cout << "[" << levelname[level] << "] ";
		(cout << ... << args) << endl;
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
inline void mkMapMv(InputIt first, InputIt last, unordered_map<K, V>& umap, F&& key)
{
	umap.clear();
	umap.reserve(distance(first, last));
	for(; first != last; ++first)
		umap.try_emplace(key(*first), move(*first));
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
inline string strjoin(InputIt first, InputIt last, const std::string_view& glue = "",F&& tf=[](InputIt it){return *it;})
{
    if(first == last)
        return string();
    string str(tf(first));
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
inline string& strjoin(string& dest, const Args&... args)
{
	((dest += args), ...);
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
 * @param      pl    { parameter_description }
 *
 * @tparam     tag   { description }
 */
template<uint32_t tag>
inline void addTagStrLine(string& dest, const ExmdbQueries::PropvalList& pl)
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
		dbpath = move(other.dbpath);
		usrpath	= move(other.usrpath);
		client = move(other.client);
		reuse = move(other.reuse);
		namedProptags = move(other.namedProptags);
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
	 * @param      userdir    Path to the users home-directory
	 * @param      exmdbHost  Host name for exmdb connection
	 * @param      exmdbPort  Port for exmdb connection
	 * @param      outpath    Path of the output database or empty for default
	 */
	explicit IndexDB(const filesystem::path& userdir, const string& exmdbHost, const string& exmdbPort, const string& outpath) :
	    usrpath(userdir),
	    client(exmdbHost, exmdbPort, userdir, true)
	{
		if(!filesystem::exists(userdir))
			throw runtime_error("Could not find user directory.");
		if(outpath.empty())
		{
			dbpath = userdir;
			dbpath /= "exmdb";
			if(!filesystem::exists(dbpath))
				throw runtime_error("Could not find users exmdb directory.");
			dbpath /= "index.sqlite3";
		}
		else
		{
			dbpath = outpath;
			if(filesystem::is_directory(dbpath))
				dbpath /= "index.sqlite3";
		}
		update = filesystem::exists(dbpath);
		msg<STATUS>(update? "Existing index found - updating" : "Creating new index for user");
		int res = sqlite3_open(dbpath.c_str(), &db);
		if(res != SQLITE_OK)
			throw runtime_error(string("Failed to open index database: ")+sqlite3_errmsg(db));
		res = sqliteExec("CREATE TABLE IF NOT EXISTS hierarchy ("
		                 " folder_id INTEGER PRIMARY KEY,"
		                 " commit_max INTEGER NOT NULL,"
		                 " max_cn INTEGER NOT NULL);\n"
		                 "CREATE VIRTUAL TABLE IF NOT EXISTS messages USING fts4 ("
		                 " sender, sending, recipients, "
		                 " subject, content, attachments,"
		                 " others, message_id PRIMARY KEY,"
		                 " attach_indexed UNINDEXED,"
		                 " entryid UNINDEXED,"
		                 " change_num UNINDEXED,"
		                 " folder_id UNINDEXED,"
		                 " message_class UNINDEXED,"
		                 " date UNINDEXED, "
		                 " tokenize=unicode61)");
		if(res != SQLITE_OK)
			throw runtime_error(string("Failed to initialize index database: ")+sqlite3_errmsg(db));
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
	using PropvalMap = unordered_map<uint32_t, structures::TaggedPropval>; ///< Tag ID -> TaggedPropval mapping

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
				throw runtime_error(sqlite3_errmsg(db));
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
			int res = func(stmt, args...);
			if(res != SQLITE_OK)
				throw runtime_error(sqlite3_errmsg(sqlite3_db_handle(stmt)));
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
			if(str.size() > numeric_limits<int>::max())
				throw out_of_range("String lengths exceeds maximum");
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
				throw out_of_range(string("Cannot find named bind parameter ")+name);
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
				throw runtime_error("SQLite query failed: "+to_string(result));
			return result;
		}

		sqlite3_stmt* stmt; ///< Managed statement
	};

	/**
	 * @brief      Helper struct for messages
	 */
	struct Message
	{
		inline Message(uint64_t mid, uint64_t fid,  structures::TaggedPropval& entryid) : mid(mid), fid(fid), entryid(move(entryid)) {}

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
		string attchs, body, other, rcpts, sender, sending, subject, messageclass;
		PropvalMap props;

		void reset()
		{
			rcpts.clear(); attchs.clear(); sending.clear(); sender.clear(); body.clear();
			props.clear();
		}
	} reuse; ///< Objects that can be reused to save on memory allocations

	static array<structures::PropertyName, 12> namedTags; ///< Array of named tags to query
	static constexpr array<uint16_t, 12> namedTagTypes = {
//	    PropvalType::STRING_ARRAY,
	    PropvalType::STRING, PropvalType::STRING, PropvalType::STRING,
	    PropvalType::STRING, PropvalType::STRING, PropvalType::STRING,
	    PropvalType::STRING, PropvalType::STRING,
	    PropvalType::STRING, PropvalType::STRING,
	    PropvalType::STRING, PropvalType::STRING,
//	    PropvalType::STRING_ARRAY
	}; ///< Types of the named tags

	static constexpr array<uint32_t, 13> msgtags1 = {
	     PropTag::ENTRYID, PropTag::SENTREPRESENTINGNAME, PropTag::SENTREPRESENTINGSMTPADDRESS,
	     PropTag::SUBJECT, PropTag::BODY, PropTag::SENDERNAME,
	     PropTag::SENDERSMTPADDRESS, PropTag::INTERNETCODEPAGE,
	     PropTag::CHANGENUMBER, PropTag::FOLDERID, PropTag::MESSAGECLASS,
	     PropTag::MESSAGEDELIVERYTIME, PropTag::LASTMODIFICATIONTIME
	}; ///< Part 1 of message tags to query

	static constexpr array<uint32_t, 19> msgtags2 = {
	    PropTag::DISPLAYNAME, PropTag::DISPLAYNAMEPREFIX, PropTag::HOMETELEPHONENUMBER,
	    PropTag::MOBILETELEPHONENUMBER, PropTag::BUSINESSTELEPHONENUMBER,
	    PropTag::BUSINESSFAXNUMBER, PropTag::ASSISTANTTELEPHONENUMBER,
	    PropTag::BUSINESS2TELEPHONENUMBER, PropTag::CALLBACKTELEPHONENUMBER,
	    PropTag::CARTELEPHONENUMBER, PropTag::COMPANYMAINTELEPHONENUMBER,
	    PropTag::HOME2TELEPHONENUMBER, PropTag::HOMEFAXNUMBER, PropTag::OTHERTELEPHONENUMBER,
	    PropTag::PAGERTELEPHONENUMBER, PropTag::PRIMARYFAXNUMBER,
	    PropTag::PRIMARYTELEPHONENUMBER, PropTag::RADIOTELEPHONENUMBER, PropTag::TELEXNUMBER,
	}; ///< Part 2 of message tags to query

	filesystem::path usrpath; ///< Path to the users home directory
	filesystem::path dbpath; ///< Path to the index database
	ExmdbClient client; ///< Exmdb client to use
	vector<uint32_t> namedProptags; ///< Store specific named proptag IDs
	sqlite3* db = nullptr; ///< SQLite database connection
	bool update = false; ///< Whether index is updated

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
		if(res == tplist.end())
			throw out_of_range("Failed to find tag.");
		return *res;
	}

	/**
	 * @brief      Check folders and messages for updates
	 *
	 * @return     Pair of messages and hierarchy entries to update
	 */
	pair<vector<Message>, vector<Hierarchy>> getUpdates()
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
		vector<Message> messages;
		vector<Hierarchy> hierarchy;
		for(auto& entry : qtResponse.entries)
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
					if(uint64_t(sqlite3_column_int64(stmt, 0)) == lctm)
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
				maxCn = max(maxCn, cn);
				messages.emplace_back(getTag(content, PropTag::MID).value.u64, folderId, getTag(content, PropTag::ENTRYID));
			}
			msg<TRACE>("Checked folder ", folderId, " with ", contents.entries.size(), " messages. ",
			           "Total updates now at ", messages.size(), ".");
			hierarchy.emplace_back(folderId, lctm, maxCn);
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
		msg<DEBUG>("Removing modified messages");
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
	void insertMessages(const vector<Message>& messages)
	{
		msg<DEBUG>("Inserting new messages");
		if(messages.empty())
			return;
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
			catch (exception& e)
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
	void insertMessage(SQLiteStmt& stmt, const Message& message, const vector<uint32_t>& msgtags)
	{
		using namespace constants;
		using namespace requests;
		static const uint32_t attchProps[] = {PropTag::ATTACHLONGFILENAME};
		msg<TRACE>("Inserting message ", message.fid, "/", message.mid);
		reuse.reset();
		stmt.call(sqlite3_reset);
		uint32_t instance = client.send<LoadMessageInstanceRequest>(usrpath, "", 65001, false, 0, message.mid).instanceId;
		auto rcpts = client.send<GetMessageInstanceRecipientsRequest>(usrpath, instance, 0, numeric_limits<uint16_t>::max());
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
		if((it = reuse.props.find(PropTag::SENDERNAME)) != reuse.props.end())
			reuse.sender += it->second.value.str;
		if((it = reuse.props.find(PropTag::SENDERSMTPADDRESS)) != reuse.props.end())
			strjoin(reuse.sender, "\n", it->second.value.str);
		if((it = reuse.props.find(PropTag::BODY)) != reuse.props.end())
			reuse.body = it->second.value.str;
		if((it = reuse.props.find(PropTag::MESSAGECLASS)) != reuse.props.end())
			reuse.messageclass = it->second.value.str;
		for(const auto& entry : namedProptags)
		{
			if((it = reuse.props.find(entry)) == reuse.props.end())
				continue;
			structures::TaggedPropval& tp = it->second;
			if(tp.type != PropvalType::STRING && tp.type != PropvalType::STRING_ARRAY)
				continue;
			//TODO: Add support for string array
			strjoin(reuse.other, reuse.other.empty()? "" : "\n", tp.type == PropvalType::STRING? tp.value.str : "");
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
		if((it = reuse.props.find(PropTag::CHANGENUMBER)) != reuse.props.end())
			stmt.call(sqlite3_bind_int64, stmt.indexOf(":change_num"), util::gcToValue(it->second.value.u64));
		if((it = reuse.props.find(PropTag::FOLDERID)) != reuse.props.end())
			stmt.call(sqlite3_bind_int64, stmt.indexOf(":folder_id"), it->second.value.u64);
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
			throw out_of_range("Number of named property IDs does not match expected count");
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

array<structures::PropertyName, 12> IndexDB::namedTags = {
//    structures::PropertyName(structures::GUID("00020329-0000-0000-C000-000000000046"), "Keywords"), //categories
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
//    structures::PropertyName(structures::GUID("00062008-0000-0000-C000-000000000046"), 0x8539)      //companies
};

///////////////////////////////////////////////////////////////////////////////////////////////////

static string exmdbHost; ///< Exmdb host to connect to
static string exmdbPort; ///< Port of the exmdb connection
static string userpath; ///< Path to the users home directory
static string outpath; ///< Index database path (empty for default)

/**
 * @brief      Print help message
 *
 * @param      name   Name of the program
 */
[[noreturn]] static void printHelp(const char* name)
{
	cout << "grommunio mailbox indexing tool\n"
	        "\nUsage: " << name << " [-e host] [-h] [-o file] [-p port] [-q] [-v] <userpath>\n"
	        "\nPositional arguments:\n"
	        "\t userpath\t\tPath to the users home directory\n"
	        "\nOptional arguments:\n"
	        "\t-e\t--host   \tHostname of the exmdb server\n"
	        "\t-h\t--help   \tShow this help message and exit\n"
	        "\t-o\t--out    \tWrite index database to specific file\n"
	        "\t-p\t--port   \tPort of the exmdb server\n"
	        "\t-q\t--quiet  \tReduce verbosity level\n"
	        "\t-v\t--verbose\tIncrease verbosity level\n";
	exit(0);
}

/**
 * @brief      Advance by one command line argument
 *
 * If there are no more arguments, exit with error.
 *
 * @param      cur   Pointer to current argument
 *
 * @return     Next argument
 */
static const char* nextArg(const char** &cur)
{
	if(*++cur == nullptr)
	{
		msg<FATAL>("Missing option argument after '", *(cur-1), "'");
		exit(RESULT_ARGERR_SYN);
	}
	return *cur;
}

/**
 * @brief      Parse command line arguments
 *
 * Does not return in case of error.
 *
 * @param      argv  nullptr-terminated array of command line arguments
 */
static void parseArgs(const char* argv[])
{
	bool noopt = false;
	for(const char** argp = argv+1; *argp != nullptr; ++argp)
	{
		const char* arg = *argp;
		if(noopt || *arg == '-')
		{
			if(*(++arg) == '-')
			{
				++arg;
				if(!strcmp(arg, "help"))		printHelp(*argv);
				else if(!strcmp(arg, "host"))	exmdbHost = nextArg(argp);
				else if(!strcmp(arg, "out"))	outpath = nextArg(argp);
				else if(!strcmp(arg, "port"))	exmdbPort = nextArg(argp);
				else if(!strcmp(arg, "quiet"))	--verbosity;
				else if(!strcmp(arg, "verbose"))++verbosity;
				else if(!*arg) noopt = true;
				else
				{
					msg<FATAL>("Unknown option '", arg, "'");
					exit(RESULT_ARGERR_SYN);
				}
			}
			else
				for(const char* sopt = arg; *sopt; ++sopt)
				{
					switch(*sopt)
					{
					case 'h': printHelp(*argv); //printHelp never returns
					case 'e': exmdbHost = nextArg(argp); break;
					case 'o': outpath = nextArg(argp); break;
					case 'p': exmdbPort = nextArg(argp); break;
					case 'q': --verbosity; break;
					case 'v': ++verbosity; break;
					default:
						msg<FATAL>("Unknown short option '", *sopt, "'");
						exit(RESULT_ARGERR_SYN);
					}
				}
		}
		else if(!userpath.empty())
		{
			msg<FATAL>("Too many arguments.");
			exit(RESULT_ARGERR_SYN);
		}
		else
			userpath = arg;
	}
	if(userpath.empty())
	{
		msg<FATAL>("No user path given");
		exit(RESULT_ARGERR_SYN);
	}
	if(exmdbHost.empty())
		exmdbHost = "localhost";
	if(exmdbPort.empty())
		exmdbPort = "5000";
	verbosity = min(max(verbosity, 0), LOGLEVELS-1);
}

int main(int, const char* argv[])
{
	parseArgs(argv);
	msg<DEBUG>("exmdb=", exmdbHost, ":", exmdbPort, ", user=", userpath, ", output=", outpath.empty()? "<default>" : outpath);
	IndexDB cache;
	try {
		cache = IndexDB(userpath, exmdbHost, exmdbPort, outpath);
		cache.refresh();
	} catch(const runtime_error& err) {
		msg<FATAL>(err.what());
		return RESULT_ARGERR_SEM;
	}
	return 0;
}
