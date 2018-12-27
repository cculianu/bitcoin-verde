$(document).ready(function() {
    const searchInput = $("#search");
    const loadingImage = $("#search-loading-image");

    searchInput.on("focus", function() {
        searchInput.select();
    });

    searchInput.on("keyup", function(event) {
        const value = searchInput.val();
        searchInput.css("text-transform", (value.length == 64 ? "uppercase" : "none"));
    });

    searchInput.on("keypress", function(event) {
        const value = searchInput.val();
        if (value.length == 0) { return true; }

        const key = event.which;
        if (key != KeyCodes.ENTER) { return true; }

        loadingImage.css("visibility", "visible");
        Api.search({ query: value }, function(data) {
            loadingImage.css("visibility", "hidden");

            const wasSuccess = data.wasSuccess;
            const errorMessage = data.errorMessage;
            const objectType = data.objectType;
            const object = data.object;

            if (wasSuccess) {
                if ( (objectType == Constants.BLOCK) || (objectType == Constants.BLOCK_HEADER) ) {
                    Ui.renderBlock(object);
                }
                else if (objectType == Constants.ADDRESS) {
                    Ui.renderAddress(object);
                }
                else if (objectType == Constants.TRANSACTION) {
                    Ui.renderTransaction(object);
                }
                else {
                    Console.log("Unknown ObjectType: " + objectType);
                }
            }
            else {
               console.log(errorMessage);
            }
        });

        searchInput.blur();

        return false;
    });   

    const queryParams = new URLSearchParams(window.location.search);
    if (queryParams.has("search")) {
        searchInput.val(queryParams.get("search"));
        searchInput.trigger($.Event( "keypress", { which: KeyCodes.ENTER } ));
    }

    window.onpopstate = function(event) {
        const state = event.state;
        if (state && state.hash) {
            searchInput.val(state.hash);
            searchInput.trigger($.Event( "keypress", { which: KeyCodes.ENTER } ));
        }
        else {
            searchInput.val("");

            const main = $("#main");
            main.empty();
        }
    };

    window.HashResizer = function(container) {
        if (container) {
            // If the container is specifically provided, do not use/reset the global timer...
            window.setTimeout(function() {
                window.HashResizer.update(container);
            }, 200);
            return;
        }

        window.clearTimeout(window.HashResizer.globalTimeout);
        window.HashResizer.globalTimeout = window.setTimeout(window.HashResizer.update, 200);
    }

    window.HashResizer.update = function(container) {
        const TEXT_NODE_TYPE = 3;
        container = (container || document);

        $(".hash, .transaction-hash, .block-hashes, .previous-block-hash", container).each(function() {
            const element = $(this);

            window.setTimeout(function() {
                const valueElement = $(".value", element);
                const textNode = (valueElement.contents().filter(function() { return this.nodeType == TEXT_NODE_TYPE; })[0] || { });
                const textNodeContent = textNode.nodeValue;

                const originalValue = (valueElement.data("hash-value") || textNodeContent || "");
                if (originalValue.length != 64) { return; } // Sanity check...

                valueElement.data("hash-value", originalValue);
                textNode.nodeValue = originalValue;

                const isOverflowing = ( (element[0].offsetHeight < element[0].scrollHeight) || (element[0].offsetWidth < element[0].scrollWidth) );
                if (isOverflowing) {
                    textNode.nodeValue = originalValue.substr(0, 10) + "..." + originalValue.substr(54);
                }
            }, 0);
        });
    };

    $(window).on("load resize", function() {
        window.HashResizer();
    });
});

