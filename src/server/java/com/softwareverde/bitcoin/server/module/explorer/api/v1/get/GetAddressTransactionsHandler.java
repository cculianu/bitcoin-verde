package com.softwareverde.bitcoin.server.module.explorer.api.v1.get;

import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.rpc.NodeJsonRpcConnection;
import com.softwareverde.bitcoin.server.module.api.ApiResult;
import com.softwareverde.bitcoin.server.module.explorer.api.Environment;
import com.softwareverde.bitcoin.server.module.explorer.api.endpoint.AddressesApi;
import com.softwareverde.http.querystring.GetParameters;
import com.softwareverde.http.server.servlet.request.Request;
import com.softwareverde.http.server.servlet.response.JsonResponse;
import com.softwareverde.http.server.servlet.response.Response;
import com.softwareverde.http.server.servlet.routed.RequestHandler;
import com.softwareverde.json.Json;
import com.softwareverde.util.Util;

import java.util.Map;

public class GetAddressTransactionsHandler implements RequestHandler<Environment> {

    protected final AddressInflater _addressInflater = new AddressInflater();

    /**
     * GET ADDRESS TRANSACTION
     * Requires GET:    address, <rawFormat>
     * Requires POST:
     */
    @Override
    public Response handleRequest(final Request request, final Environment environment, final Map<String, String> urlParameters) throws Exception {
        final String addressString;
        final Address address;
        {
            final GetParameters getParameters = request.getGetParameters();
            addressString = Util.coalesce(urlParameters.get("address"), getParameters.get("address"));
            if (! Util.isBlank(addressString)) {
                final Address base58Address = _addressInflater.fromBase58Check(addressString);
                final Address base32Address = _addressInflater.fromBase32Check(addressString);
                address = Util.coalesce(base58Address, base32Address);
            }
            else {
                address = null;
            }
        }

        final Boolean rawFormat;
        {
            final GetParameters getParameters = request.getGetParameters();
            final String rawFormatString = Util.coalesce(urlParameters.get("rawFormat"), getParameters.get("rawFormat"));
            if (! Util.isBlank(rawFormatString)) {
                rawFormat = Util.parseBool(rawFormatString);
            }
            else {
                rawFormat = false;
            }
        }

        if (address == null) {
            final AddressesApi.GetTransactionsResult result = new AddressesApi.GetTransactionsResult();
            result.setWasSuccess(false);
            result.setErrorMessage("Invalid address parameter: " + addressString);
            return new JsonResponse(Response.Codes.BAD_REQUEST, result);
        }

        try (final NodeJsonRpcConnection nodeJsonRpcConnection = environment.getNodeJsonRpcConnection()) {
            if (nodeJsonRpcConnection == null) {
                final AddressesApi.GetTransactionsResult result = new AddressesApi.GetTransactionsResult();
                result.setWasSuccess(false);
                result.setErrorMessage("Unable to connect to node.");
                return new JsonResponse(Response.Codes.SERVER_ERROR, result);
            }

            final Json transactionsJson;
            {
                final Json rpcResponseJson = nodeJsonRpcConnection.getAddressTransactions(address, rawFormat);
                if (rpcResponseJson == null) {
                    return new JsonResponse(Response.Codes.SERVER_ERROR, new ApiResult(false, "Request timed out."));
                }

                if (! rpcResponseJson.getBoolean("wasSuccess")) {
                    final String errorMessage = rpcResponseJson.getString("errorMessage");
                    return new JsonResponse(Response.Codes.SERVER_ERROR, new ApiResult(false, errorMessage));
                }

                transactionsJson = rpcResponseJson.get("transactions");
            }

            final AddressesApi.GetTransactionsResult getTransactionResult = new AddressesApi.GetTransactionsResult();
            getTransactionResult.setWasSuccess(true);
            getTransactionResult.setTransactionsJson(transactionsJson);
            return new JsonResponse(Response.Codes.OK, getTransactionResult);
        }
    }
}
