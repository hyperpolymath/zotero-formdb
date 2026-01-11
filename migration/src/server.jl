# SPDX-License-Identifier: AGPL-3.0-or-later
"""
Zotero-Compatible REST API Server

Serves FormDB journal data through Zotero's REST API endpoints.
This allows existing Zotero clients to work with FormDB storage.

Endpoints implemented:
  GET  /users/:userID/items           - List all items
  GET  /users/:userID/items/:key      - Get single item
  GET  /users/:userID/collections     - List all collections
  GET  /users/:userID/collections/:key - Get single collection
  GET  /users/:userID/items/:key/children - Get item's attachments/notes
  POST /users/:userID/items           - Create items
  PUT  /users/:userID/items/:key      - Update item
  DELETE /users/:userID/items/:key    - Delete item

Query parameters supported:
  format=json (default)
  limit=100 (default, max 100)
  start=0 (pagination offset)
  sort=dateModified (default)
  direction=desc (default)
  itemType=book,journalArticle,...
  q=search query
"""
module ZoteroServer

using HTTP
using JSON3
using Dates
using SHA
using UUIDs

export start_server, ServerConfig

# Server configuration
Base.@kwdef struct ServerConfig
    journal_dir::String
    port::Int = 8080
    host::String = "127.0.0.1"
    user_id::String = "local"
    api_key::Union{String, Nothing} = nothing
end

# In-memory index for fast lookups
mutable struct JournalIndex
    items::Dict{String, Dict{String, Any}}
    collections::Dict{String, Dict{String, Any}}
    attachments::Dict{String, Vector{Dict{String, Any}}}
    notes::Dict{String, Vector{Dict{String, Any}}}
    collection_items::Dict{String, Vector{String}}
    last_version::Int
    lock::ReentrantLock
end

JournalIndex() = JournalIndex(
    Dict{String, Dict{String, Any}}(),
    Dict{String, Dict{String, Any}}(),
    Dict{String, Vector{Dict{String, Any}}}(),
    Dict{String, Vector{Dict{String, Any}}}(),
    Dict{String, Vector{String}}(),
    0,
    ReentrantLock()
)

# Global state
const JOURNAL_INDEX = Ref{JournalIndex}(JournalIndex())
const SERVER_CONFIG = Ref{ServerConfig}(ServerConfig(journal_dir="."))

"""
Load journal into memory index for fast queries.
"""
function load_journal!(config::ServerConfig)
    journal_path = joinpath(config.journal_dir, "journal.jsonl")

    if !isfile(journal_path)
        @warn "Journal not found" path=journal_path
        return
    end

    index = JournalIndex()

    open(journal_path, "r") do io
        for line in eachline(io)
            isempty(strip(line)) && continue

            try
                entry = JSON3.read(line, Dict{String, Any})
                process_entry!(index, entry)
            catch e
                @warn "Failed to parse journal entry" error=e
            end
        end
    end

    JOURNAL_INDEX[] = index
    @info "Journal loaded" items=length(index.items) collections=length(index.collections)
end

"""
Process a single journal entry into the index.

Handles two formats:
1. Migration format: {op_type, collection, payload (JSON string), ...}
2. API format: {type, data, ...}
"""
function process_entry!(index::JournalIndex, entry::Dict{String, Any})
    seq = get(entry, "sequence", 0)

    # Detect format and extract entry type and data
    local entry_type::String
    local data::Dict{String, Any}

    if haskey(entry, "collection")
        # Migration format: collection field indicates type
        collection = get(entry, "collection", "")
        payload_str = get(entry, "payload", "{}")

        # Parse payload JSON string
        data = try
            JSON3.read(payload_str, Dict{String, Any})
        catch
            Dict{String, Any}()
        end

        # Map collection name to entry type
        entry_type = if collection == "collections"
            "collection"
        elseif collection == "items"
            "item"
        elseif collection == "attachments"
            "attachment"
        elseif collection == "notes"
            "note"
        elseif collection == "collection_items"
            "collection_item"
        else
            collection
        end
    else
        # API format: type and data fields
        entry_type = get(entry, "type", "")
        data = get(entry, "data", Dict{String, Any}())
    end

    lock(index.lock) do
        index.last_version = max(index.last_version, seq)

        if entry_type == "item"
            key = get(data, "key", "")
            if !isempty(key)
                index.items[key] = data
            end
        elseif entry_type == "attachment"
            key = get(data, "key", "")
            parent_key = get(data, "parentItem", "")
            if !isempty(key)
                index.items[key] = data
                if !isempty(parent_key)
                    if !haskey(index.attachments, parent_key)
                        index.attachments[parent_key] = Vector{Dict{String, Any}}()
                    end
                    push!(index.attachments[parent_key], data)
                end
            end
        elseif entry_type == "note"
            key = get(data, "key", "")
            parent_key = get(data, "parentItem", "")
            if !isempty(key)
                index.items[key] = data
                if !isempty(parent_key)
                    if !haskey(index.notes, parent_key)
                        index.notes[parent_key] = Vector{Dict{String, Any}}()
                    end
                    push!(index.notes[parent_key], data)
                end
            end
        elseif entry_type == "collection"
            key = get(data, "key", "")
            if !isempty(key)
                index.collections[key] = data
            end
        elseif entry_type == "collection_item"
            collection_key = get(data, "collectionKey", "")
            item_key = get(data, "itemKey", "")
            if !isempty(collection_key) && !isempty(item_key)
                if !haskey(index.collection_items, collection_key)
                    index.collection_items[collection_key] = Vector{String}()
                end
                push!(index.collection_items[collection_key], item_key)
            end
        end
    end
end

"""
Convert FormDB item to Zotero API format.
"""
function to_zotero_format(item::Dict{String, Any}, version::Int)
    key = get(item, "key", "")
    item_type = get(item, "itemType", "unknown")

    # Build Zotero-compatible response
    response = Dict{String, Any}(
        "key" => key,
        "version" => version,
        "library" => Dict(
            "type" => "user",
            "id" => SERVER_CONFIG[].user_id,
            "name" => "FormDB Library"
        ),
        "data" => Dict{String, Any}(
            "key" => key,
            "version" => version,
            "itemType" => item_type
        )
    )

    # Copy over standard fields
    data = response["data"]
    for field in ["title", "creators", "abstractNote", "date", "url", "accessDate",
                  "DOI", "ISBN", "ISSN", "pages", "volume", "issue", "publisher",
                  "publicationTitle", "journalAbbreviation", "language", "rights",
                  "extra", "dateAdded", "dateModified", "tags", "collections",
                  "relations", "parentItem", "note", "contentType", "charset",
                  "filename", "md5", "mtime", "linkMode", "path"]
        if haskey(item, field)
            data[field] = item[field]
        end
    end

    # Add provenance metadata as extra field if present
    if haskey(item, "provenance")
        prov = item["provenance"]
        existing_extra = get(data, "extra", "")
        prov_extra = "FormDB-Actor: $(get(prov, "actor", "unknown"))\nFormDB-Rationale: $(get(prov, "rationale", ""))"
        data["extra"] = isempty(existing_extra) ? prov_extra : "$existing_extra\n$prov_extra"
    end

    return response
end

"""
Convert FormDB collection to Zotero API format.
"""
function collection_to_zotero_format(coll::Dict{String, Any}, version::Int)
    key = get(coll, "key", "")

    Dict{String, Any}(
        "key" => key,
        "version" => version,
        "library" => Dict(
            "type" => "user",
            "id" => SERVER_CONFIG[].user_id,
            "name" => "FormDB Library"
        ),
        "data" => Dict{String, Any}(
            "key" => key,
            "version" => version,
            "name" => get(coll, "name", "Untitled"),
            "parentCollection" => get(coll, "parentKey", nothing)
        )
    )
end

# Request handlers

function handle_get_items(req::HTTP.Request, params::Dict{String, String})
    index = JOURNAL_INDEX[]
    version = index.last_version

    # Parse query parameters
    uri = HTTP.URI(req.target)
    query = HTTP.queryparams(uri)

    limit = min(parse(Int, get(query, "limit", "100")), 100)
    start = parse(Int, get(query, "start", "0"))
    item_type_filter = get(query, "itemType", nothing)
    search_query = get(query, "q", nothing)

    # Filter and collect items (excluding attachments and notes as top-level)
    items = Vector{Dict{String, Any}}()

    lock(index.lock) do
        for (key, item) in index.items
            item_type = get(item, "itemType", "")

            # Skip attachments and notes in top-level listing
            if item_type in ["attachment", "note"]
                continue
            end

            # Apply item type filter
            if !isnothing(item_type_filter)
                types = split(item_type_filter, ",")
                if !(item_type in types)
                    continue
                end
            end

            # Apply search filter
            if !isnothing(search_query)
                title = lowercase(get(item, "title", ""))
                if !contains(title, lowercase(search_query))
                    continue
                end
            end

            push!(items, to_zotero_format(item, version))
        end
    end

    # Sort by dateModified (desc)
    sort!(items, by=x -> get(get(x, "data", Dict()), "dateModified", ""), rev=true)

    # Paginate
    total = length(items)
    items = items[start+1:min(start+limit, total)]

    # Build response with headers
    headers = [
        "Content-Type" => "application/json",
        "Zotero-API-Version" => "3",
        "Total-Results" => string(total),
        "Last-Modified-Version" => string(version)
    ]

    return HTTP.Response(200, headers, JSON3.write(items))
end

function handle_get_item(req::HTTP.Request, params::Dict{String, String})
    key = get(params, "key", "")
    index = JOURNAL_INDEX[]
    version = index.last_version

    item = lock(index.lock) do
        get(index.items, key, nothing)
    end

    if isnothing(item)
        return HTTP.Response(404, ["Content-Type" => "application/json"],
                            JSON3.write(Dict("error" => "Item not found")))
    end

    response = to_zotero_format(item, version)

    headers = [
        "Content-Type" => "application/json",
        "Zotero-API-Version" => "3",
        "Last-Modified-Version" => string(version)
    ]

    return HTTP.Response(200, headers, JSON3.write(response))
end

function handle_get_item_children(req::HTTP.Request, params::Dict{String, String})
    key = get(params, "key", "")
    index = JOURNAL_INDEX[]
    version = index.last_version

    children = Vector{Dict{String, Any}}()

    lock(index.lock) do
        # Get attachments
        for att in get(index.attachments, key, [])
            push!(children, to_zotero_format(att, version))
        end
        # Get notes
        for note in get(index.notes, key, [])
            push!(children, to_zotero_format(note, version))
        end
    end

    headers = [
        "Content-Type" => "application/json",
        "Zotero-API-Version" => "3",
        "Total-Results" => string(length(children)),
        "Last-Modified-Version" => string(version)
    ]

    return HTTP.Response(200, headers, JSON3.write(children))
end

function handle_get_collections(req::HTTP.Request, params::Dict{String, String})
    index = JOURNAL_INDEX[]
    version = index.last_version

    collections = Vector{Dict{String, Any}}()

    lock(index.lock) do
        for (key, coll) in index.collections
            push!(collections, collection_to_zotero_format(coll, version))
        end
    end

    # Sort by name
    sort!(collections, by=x -> get(get(x, "data", Dict()), "name", ""))

    headers = [
        "Content-Type" => "application/json",
        "Zotero-API-Version" => "3",
        "Total-Results" => string(length(collections)),
        "Last-Modified-Version" => string(version)
    ]

    return HTTP.Response(200, headers, JSON3.write(collections))
end

function handle_get_collection(req::HTTP.Request, params::Dict{String, String})
    key = get(params, "key", "")
    index = JOURNAL_INDEX[]
    version = index.last_version

    coll = lock(index.lock) do
        get(index.collections, key, nothing)
    end

    if isnothing(coll)
        return HTTP.Response(404, ["Content-Type" => "application/json"],
                            JSON3.write(Dict("error" => "Collection not found")))
    end

    response = collection_to_zotero_format(coll, version)

    headers = [
        "Content-Type" => "application/json",
        "Zotero-API-Version" => "3",
        "Last-Modified-Version" => string(version)
    ]

    return HTTP.Response(200, headers, JSON3.write(response))
end

function handle_get_collection_items(req::HTTP.Request, params::Dict{String, String})
    collection_key = get(params, "key", "")
    index = JOURNAL_INDEX[]
    version = index.last_version

    items = Vector{Dict{String, Any}}()

    lock(index.lock) do
        item_keys = get(index.collection_items, collection_key, String[])
        for key in item_keys
            item = get(index.items, key, nothing)
            if !isnothing(item)
                push!(items, to_zotero_format(item, version))
            end
        end
    end

    headers = [
        "Content-Type" => "application/json",
        "Zotero-API-Version" => "3",
        "Total-Results" => string(length(items)),
        "Last-Modified-Version" => string(version)
    ]

    return HTTP.Response(200, headers, JSON3.write(items))
end

# Write operations - append to journal

function append_to_journal(entry::Dict{String, Any})
    config = SERVER_CONFIG[]
    journal_path = joinpath(config.journal_dir, "journal.jsonl")

    index = JOURNAL_INDEX[]

    new_seq = lock(index.lock) do
        index.last_version += 1
        index.last_version
    end

    # Build journal entry with provenance
    journal_entry = Dict{String, Any}(
        "sequence" => new_seq,
        "timestamp" => Dates.format(now(UTC), "yyyy-mm-ddTHH:MM:SSZ"),
        "type" => entry["type"],
        "data" => entry["data"],
        "provenance" => Dict(
            "actor" => "zotero-api-client",
            "rationale" => get(entry, "rationale", "API write operation")
        )
    )

    # Compute hash chain
    prev_hash = ""
    if isfile(journal_path)
        # Read last line for previous hash
        lines = readlines(journal_path)
        if !isempty(lines)
            last_entry = JSON3.read(lines[end], Dict{String, Any})
            prev_hash = get(last_entry, "hash", "")
        end
    end

    content_hash = bytes2hex(sha256(JSON3.write(journal_entry)))
    journal_entry["prev_hash"] = prev_hash
    journal_entry["hash"] = bytes2hex(sha256(prev_hash * content_hash))

    # Append to journal
    open(journal_path, "a") do io
        println(io, JSON3.write(journal_entry))
    end

    # Update in-memory index
    process_entry!(index, journal_entry)

    return new_seq
end

function handle_post_items(req::HTTP.Request, params::Dict{String, String})
    try
        body = JSON3.read(String(req.body), Vector{Dict{String, Any}})

        created_items = Vector{Dict{String, Any}}()

        for item_data in body
            # Generate key if not provided
            if !haskey(item_data, "key")
                item_data["key"] = uppercase(randstring(['A':'Z'; '0':'9'], 8))
            end

            # Set timestamps
            now_str = Dates.format(now(UTC), "yyyy-mm-ddTHH:MM:SSZ")
            item_data["dateAdded"] = get(item_data, "dateAdded", now_str)
            item_data["dateModified"] = now_str

            # Append to journal
            new_version = append_to_journal(Dict(
                "type" => "item",
                "data" => item_data,
                "rationale" => "Created via Zotero API"
            ))

            push!(created_items, to_zotero_format(item_data, new_version))
        end

        headers = [
            "Content-Type" => "application/json",
            "Zotero-API-Version" => "3",
            "Last-Modified-Version" => string(JOURNAL_INDEX[].last_version)
        ]

        return HTTP.Response(200, headers, JSON3.write(Dict(
            "successful" => Dict(string(i-1) => item for (i, item) in enumerate(created_items)),
            "success" => Dict(string(i-1) => item["key"] for (i, item) in enumerate(created_items)),
            "unchanged" => Dict(),
            "failed" => Dict()
        )))

    catch e
        @error "Failed to create items" exception=e
        return HTTP.Response(400, ["Content-Type" => "application/json"],
                            JSON3.write(Dict("error" => string(e))))
    end
end

function handle_put_item(req::HTTP.Request, params::Dict{String, String})
    key = get(params, "key", "")

    try
        item_data = JSON3.read(String(req.body), Dict{String, Any})
        item_data["key"] = key
        item_data["dateModified"] = Dates.format(now(UTC), "yyyy-mm-ddTHH:MM:SSZ")

        new_version = append_to_journal(Dict(
            "type" => "item",
            "data" => item_data,
            "rationale" => "Updated via Zotero API"
        ))

        headers = [
            "Content-Type" => "application/json",
            "Zotero-API-Version" => "3",
            "Last-Modified-Version" => string(new_version)
        ]

        return HTTP.Response(204, headers, "")

    catch e
        @error "Failed to update item" exception=e
        return HTTP.Response(400, ["Content-Type" => "application/json"],
                            JSON3.write(Dict("error" => string(e))))
    end
end

function handle_delete_item(req::HTTP.Request, params::Dict{String, String})
    key = get(params, "key", "")

    # In FormDB, we don't really delete - we mark as deleted with provenance
    new_version = append_to_journal(Dict(
        "type" => "deletion",
        "data" => Dict("key" => key, "deleted" => true),
        "rationale" => "Deleted via Zotero API"
    ))

    # Remove from index
    index = JOURNAL_INDEX[]
    lock(index.lock) do
        delete!(index.items, key)
    end

    headers = [
        "Content-Type" => "application/json",
        "Zotero-API-Version" => "3",
        "Last-Modified-Version" => string(new_version)
    ]

    return HTTP.Response(204, headers, "")
end

# Router

function route_request(req::HTTP.Request)
    path = HTTP.URI(req.target).path
    method = req.method

    # Parse path to extract user ID and resource
    # Pattern: /users/:userID/items/:key?
    #          /users/:userID/collections/:key?

    parts = split(path, "/", keepempty=false)

    if length(parts) < 3 || parts[1] != "users"
        return HTTP.Response(404, ["Content-Type" => "application/json"],
                            JSON3.write(Dict("error" => "Not found")))
    end

    user_id = parts[2]
    resource = parts[3]
    key = length(parts) >= 4 ? parts[4] : nothing
    sub_resource = length(parts) >= 5 ? parts[5] : nothing

    params = Dict{String, String}("userID" => user_id)
    if !isnothing(key)
        params["key"] = key
    end

    # Route based on method and path
    if resource == "items"
        if method == "GET"
            if isnothing(key)
                return handle_get_items(req, params)
            elseif sub_resource == "children"
                return handle_get_item_children(req, params)
            else
                return handle_get_item(req, params)
            end
        elseif method == "POST" && isnothing(key)
            return handle_post_items(req, params)
        elseif method == "PUT" && !isnothing(key)
            return handle_put_item(req, params)
        elseif method == "DELETE" && !isnothing(key)
            return handle_delete_item(req, params)
        end
    elseif resource == "collections"
        if method == "GET"
            if isnothing(key)
                return handle_get_collections(req, params)
            elseif sub_resource == "items"
                return handle_get_collection_items(req, params)
            else
                return handle_get_collection(req, params)
            end
        end
    end

    return HTTP.Response(405, ["Content-Type" => "application/json"],
                        JSON3.write(Dict("error" => "Method not allowed")))
end

function handle_request(req::HTTP.Request)
    # CORS headers for browser clients
    cors_headers = [
        "Access-Control-Allow-Origin" => "*",
        "Access-Control-Allow-Methods" => "GET, POST, PUT, DELETE, OPTIONS",
        "Access-Control-Allow-Headers" => "Content-Type, Zotero-API-Key, Zotero-API-Version"
    ]

    # Handle preflight
    if req.method == "OPTIONS"
        return HTTP.Response(204, cors_headers, "")
    end

    # Check API key if configured
    config = SERVER_CONFIG[]
    if !isnothing(config.api_key)
        provided_key = HTTP.header(req, "Zotero-API-Key", "")
        if provided_key != config.api_key
            return HTTP.Response(403, ["Content-Type" => "application/json"],
                                JSON3.write(Dict("error" => "Invalid API key")))
        end
    end

    try
        response = route_request(req)
        # Add CORS headers to response
        for (k, v) in cors_headers
            HTTP.setheader(response, k => v)
        end
        return response
    catch e
        @error "Request failed" exception=e
        return HTTP.Response(500, ["Content-Type" => "application/json"],
                            JSON3.write(Dict("error" => "Internal server error")))
    end
end

"""
Start the Zotero-compatible API server.
"""
function start_server(config::ServerConfig)
    SERVER_CONFIG[] = config

    @info "Loading journal..." dir=config.journal_dir
    load_journal!(config)

    @info "Starting Zotero API server" host=config.host port=config.port
    @info "Endpoints available:"
    @info "  GET  /users/$(config.user_id)/items"
    @info "  GET  /users/$(config.user_id)/items/:key"
    @info "  GET  /users/$(config.user_id)/items/:key/children"
    @info "  GET  /users/$(config.user_id)/collections"
    @info "  GET  /users/$(config.user_id)/collections/:key"
    @info "  GET  /users/$(config.user_id)/collections/:key/items"
    @info "  POST /users/$(config.user_id)/items"
    @info "  PUT  /users/$(config.user_id)/items/:key"
    @info "  DELETE /users/$(config.user_id)/items/:key"

    HTTP.serve(handle_request, config.host, config.port)
end

end # module
