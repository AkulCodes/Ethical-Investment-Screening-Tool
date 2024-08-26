#include <iostream>
#include <string>
#include <curl/curl.h>
#include <json/json.h>
#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <chrono>
#include <thread>
#include <vector>
#include <algorithm>

// write data received from libcurl to a string
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* s) {
    size_t newLength = size * nmemb;
    try {
        s->append((char*)contents, newLength);
    } catch (std::bad_alloc& e) {
        return 0;
    }
    return newLength;
}

// Function to make HTTP GET request using libcurl
std::string makeGetRequest(const std::string& url) {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;

    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();

    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
        res = curl_easy_perform(curl);

        if (res != CURLE_OK)
            std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << std::endl;

        curl_easy_cleanup(curl);
    }

    curl_global_cleanup();
    return readBuffer;
}

// Function to parse JSON data using Jsoncpp
std::vector<std::pair<std::string, double>> parseJsonResponse(const std::string& jsonResponse) {
    Json::CharReaderBuilder readerBuilder;
    Json::Value root;
    std::string errs;

    std::istringstream s(jsonResponse);
    std::string doc;
    s >> doc;

    if (!Json::parseFromStream(readerBuilder, s, &root, &errs)) {
        std::cerr << "Failed to parse the JSON response" << std::endl;
        return {};
    }

    std::vector<std::pair<std::string, double>> companies;
    for (const auto& company : root["companies"]) {
        std::string name = company["name"].asString();
        double esgScore = company["esg_score"].asDouble();
        companies.emplace_back(name, esgScore);
    }

    return companies;
}

// store ESG data into MySQL
void storeDataToMySQL(const std::vector<std::pair<std::string, double>>& companies) {
    sql::mysql::MySQL_Driver* driver;
    sql::Connection* con;
    sql::PreparedStatement* pstmt;

    driver = sql::mysql::get_mysql_driver_instance();
    con = driver->connect("tcp://127.0.0.1:3306", "user", "password");
    con->setSchema("esg_database");

    pstmt = con->prepareStatement("INSERT INTO companies (name, esg_score) VALUES (?, ?)");

    for (const auto& company : companies) {
        pstmt->setString(1, company.first);
        pstmt->setDouble(2, company.second);
        pstmt->execute();
    }

    delete pstmt;
    delete con;
}

// retrieve and display the top 20 companies with the highest ESG scores
void displayTopCompanies() {
    sql::mysql::MySQL_Driver* driver;
    sql::Connection* con;
    sql::Statement* stmt;
    sql::ResultSet* res;

    driver = sql::mysql::get_mysql_driver_instance();
    con = driver->connect("tcp://127.0.0.1:3306", "user", "password");
    con->setSchema("esg_database");

    stmt = con->createStatement();
    res = stmt->executeQuery("SELECT name, esg_score FROM companies ORDER BY esg_score DESC LIMIT 20");

    std::cout << "Top 20 companies with the highest ESG scores:" << std::endl;
    while (res->next()) {
        std::cout << "Company: " << res->getString("name")
                  << " | ESG Score: " << res->getDouble("esg_score") << std::endl;
    }

    delete res;
    delete stmt;
    delete con;
}

int main() {
    const std::string apiUrl = ""; // Replace with actual API URL
    const int maxRequestsPerMinute = 100;
    const int requestInterval = 60000 / maxRequestsPerMinute; // Time interval between requests in milliseconds

    for (int i = 0; i < maxRequestsPerMinute; ++i) {
        auto start = std::chrono::high_resolution_clock::now();

        std::string jsonResponse = makeGetRequest(apiUrl);
        std::vector<std::pair<std::string, double>> companies = parseJsonResponse(jsonResponse);
        storeDataToMySQL(companies);

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> requestDuration = end - start;
        std::cout << "API request time: " << requestDuration.count() << " ms" << std::endl;

        if (requestDuration.count() < requestInterval) {
            std::this_thread::sleep_for(std::chrono::milliseconds(requestInterval) - requestDuration);
        }
    }

    displayTopCompanies();
    return 0;
}
