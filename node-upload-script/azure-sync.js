var FeedParser = require('feedparser');
var fs = require('fs');
var S = require('string');
var AzureSearch = require('azure-search');
var each = require('async-each-series');
var argv = require('minimist')(process.argv.slice(2));

var rssPath = argv['rss'];
var searchUrl = argv['search-url'];
var searchKey = argv['search-key'];

var posts = [];
var feedparser = new FeedParser();

var searchClient = AzureSearch({
    url: searchUrl,
    key: searchKey,
    version: '2015-02-28-preview'
});


var readStream = fs.createReadStream(rssPath);


readStream.on('open', function () {
    readStream.pipe(feedparser);
});

readStream.on('error', function(err) {
    console.error("Couldn't open file." + err);
});


feedparser.on('readable', feedparserReadItem);

feedparser.on('end', function(err) {
    console.log("Finished reading posts: " + posts.length);
    
    rebuildSearchIndex(posts);
});

feedparser.on('error', function(err) {
    console.error("Couldn't read rss file. " + err);
});



function feedparserReadItem() {
    var stream = this;
    var meta = stream.meta;
    var item, slug;
    
    while (item = stream.read()) {
        slug = item.link.substring(item.link.lastIndexOf('/') + 1);
        posts.push({
            id: slug, 
            title: item.title, 
            content: S(item.description).stripTags().decodeHTMLEntities().s,
            link: item.link,
            categories: item.categories,
            pubdate: item.pubdate
        });
    }
}

function rebuildSearchIndex(posts) {
    var indexName = 'blog-posts';
    
    var schema = {
        name: indexName,
        fields: [
            { 
                name: 'id',
                type: 'Edm.String',
                searchable: false,
                filterable: true,
                retrievable: true,
                sortable: true,
                facetable: false,
                key: true 
            },
            { 
                name: 'title',
                type: 'Edm.String',
                searchable: true,
                filterable: true,
                retrievable: true,
                sortable: true,
                facetable: false,
                key: false,
                analyzer: 'en.microsoft'
            },
            { 
                name: 'content',
                type: 'Edm.String',
                searchable: true,
                filterable: true,
                retrievable: true,
                sortable: true,
                facetable: false,
                key: false,
                analyzer: 'en.microsoft'
            },
            { 
                name: 'link',
                type: 'Edm.String',
                searchable: false,
                filterable: false,
                retrievable: true,
                sortable: true,
                facetable: false,
                key: false 
            },
            { 
                name: 'categories',
                type: 'Collection(Edm.String)',
                searchable: true,
                filterable: true,
                retrievable: true,
                sortable: false,
                facetable: true,
                key: false 
            },
            { 
                name: 'pubdate',
                type: 'Edm.DateTimeOffset',
                searchable: false,
                filterable: true,
                retrievable: true,
                sortable: false,
                facetable: true,
                key: false 
            }
        ],
        suggesters: [
            {
                name: 'main',
                searchMode: 'analyzingInfixMatching',
                sourceFields: ['categories']
            }
        ],
        scoringProfiles: [],
        defaultScoringProfile: null,
        corsOptions: {
            allowedOrigins: ['*']
        }
    };
    
    console.log('Deleting index...');
    searchClient.deleteIndex(indexName, function (err) {
        if (err) console.error(err);
        
        console.log('Creating index...')
        searchClient.createIndex(schema, function (err, schema) {
            if (err) {
                console.dir(err);
                throw err;
            }
            
            each(posts, function(post, next) {
                console.log('Adding', post.title, '...')
                searchClient.addDocuments(indexName, [post], function (err, details) {
                    console.log(err || (details.length && details[0].status ? 'OK' : 'failed'));
                    next(err, details);
                });
            }, function (err) {
                console.log('Finished rebuilding index.');
            });
        });
    });
}
