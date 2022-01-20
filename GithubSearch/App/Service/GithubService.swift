//
//  GithubService.swift
//  GithubSearch
//
//  Created by Tomas Baculák on 07/01/2022.
//

import Foundation
import RxSwift

// Header
extension GithubService {
    struct HeaderKey {
        static let authorization = "Authorization"
    }

    struct HeaderValue {
        static let authorization = AppDefaults.apiToken
    }
}
// Query
extension GithubService {
    struct QueryItems {
        static let query = "q"
        static let sort = "sort"
        static let order = "order"
        static let perPage = "per_page"
        static let page = "page"
    }
}
// Host
extension GithubService {
    var host: String { "https://api.github.com" }
    var searchPath: String  { "/search" }
    var repositoryPath: String { "/repositories" }

    var searchRepositoriesUrlComponents: URLComponents {
        var url = URLComponents(string: ("\(host)\(searchPath)\(repositoryPath)"))!
        url.queryItems = [URLQueryItem]()
        return url
    }

    func authorizedRequest(with url: URLComponents) -> URLRequest {
        var request = URLRequest(url: url.url!)
        request.addValue(HeaderValue.authorization, forHTTPHeaderField: HeaderKey.authorization)
        return request
    }
}
// Error
extension GithubService {
    enum Error: Swift.Error {
        case invalidResponse(URLResponse?)
        case invalidJSON(Swift.Error)
        case invalidRequest(URLRequest)
        case unauthorizedRequest(URLRequest)
        case invalidURL(URL?)
        case serviceUnvailable(URL?)
        case serviceForbidden(URL?)
        case useCache
        case other
    }
}

extension GithubService.Error: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .other:
            return NSLocalizedString("Undefined error occured!", comment: "My error")
        case .invalidResponse(_):
            return NSLocalizedString("Invalid response!", comment: "My error")
        case .invalidJSON(_):
            return NSLocalizedString("JSON response not valid!.", comment: "My error")
        case .invalidRequest(_):
            return NSLocalizedString("Invalid url or you might have reached the API limits", comment: "My error")
        case .invalidURL(_):
            return NSLocalizedString("Undefined error occured!)", comment: "My error")
        case .serviceUnvailable(_):
            return NSLocalizedString("Service not available.", comment: "My error")
        case .serviceForbidden(_):
            return NSLocalizedString("Service forbidden).", comment: "My error")
        case .useCache:
            return NSLocalizedString("Cache data layer not implemented!", comment: "My error")
        case .unauthorizedRequest(_):
            return NSLocalizedString("Unauthorized request! You may have forgotten to update AppDefaults with your personal token.", comment: "My error")
        }
    }
}

class GithubService {
    func process<T: Codable>(request: URLRequest) -> Observable<T> {
        URLSession.shared.rx
            .response(request: request)
            .map { result -> Data in
                switch result.response.statusCode {
                case 200 ..< 300:
                    return result.data
                case 304:
                    throw Error.useCache
                case 401:
                    throw Error.unauthorizedRequest(request)
                case 403:
                    throw Error.serviceForbidden(request.url)
                case 404:
                    throw Error.invalidURL(request.url)
                case 422:
                    throw Error.invalidRequest(request)
                case 503:
                    throw Error.serviceUnvailable(request.url)
                default:
                    throw Error.other
                }
            }.map { data in
                do {
                    let result = try JSONDecoder().decode(T.self, from: data)
                    return result
                } catch let error {
                    throw Error.invalidJSON(error)
                }
            }.observe(on: MainScheduler.instance)
            .asObservable()
    }
}