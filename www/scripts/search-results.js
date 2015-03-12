(function() {
    
    var params = queryString.parse(location.search);
    var searchQuery = params.q;
    
    var vm = new SearchResultsViewModel(searchQuery, new BlogSearchService());
    
    ko.applyBindings(vm);
    
    
    
    
    function SearchResultsViewModel(searchQuery, searchService) {
        var vm = this;
        
        vm.searchQuery = searchQuery; // 1-time binding
        vm.results = ko.observableArray([]);
        vm.isSearching = ko.observable(false);
        
        init()
        
        
        function init() {
            if (searchQuery) {
                vm.isSearching(true);
                
                searchService.search(searchQuery, function(err, results) {
                    vm.isSearching(false);
                    if (err) return;
                    
                    vm.results(results);
                });
            }
        }
    }
    
    function BlogSearchService() {
        var svc = this;
        
        var indexName = 'blog-posts';
        var client = AzureSearch({
            url: 'https://blog-search.search.windows.net',
            key:'E5519FA720B2A6E0DD321E6D12B17072',
            version: '2015-02-28-preview'
        });
        
        svc.search = search;

        
        function search(query, callback) {
            var searchOptions = { search: query, '$select': 'id, title, link, pubdate' };
            client.search(indexName, searchOptions, callback);
        }
    }
}());
