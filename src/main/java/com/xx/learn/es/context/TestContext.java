package com.xx.learn.es.context;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.AnalyzerProviderFactory;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.PreBuiltAnalyzerProviderFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.core.BinaryFieldMapper;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.mapper.core.ByteFieldMapper;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.FloatFieldMapper;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.ShortFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.core.TokenCountFieldMapper;
import org.elasticsearch.index.mapper.core.TypeParsers;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.AndQueryParser;
import org.elasticsearch.index.query.BoolQueryParser;
import org.elasticsearch.index.query.BoostingQueryParser;
import org.elasticsearch.index.query.CommonTermsQueryParser;
import org.elasticsearch.index.query.ConstantScoreQueryParser;
import org.elasticsearch.index.query.DisMaxQueryParser;
import org.elasticsearch.index.query.ExistsQueryParser;
import org.elasticsearch.index.query.FQueryFilterParser;
import org.elasticsearch.index.query.FieldMaskingSpanQueryParser;
import org.elasticsearch.index.query.FilteredQueryParser;
import org.elasticsearch.index.query.FuzzyQueryParser;
import org.elasticsearch.index.query.GeoBoundingBoxQueryParser;
import org.elasticsearch.index.query.GeoDistanceQueryParser;
import org.elasticsearch.index.query.GeoDistanceRangeQueryParser;
import org.elasticsearch.index.query.GeoPolygonQueryParser;
import org.elasticsearch.index.query.GeoShapeQueryParser;
import org.elasticsearch.index.query.GeohashCellQuery;
import org.elasticsearch.index.query.IdsQueryParser;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.LimitQueryParser;
import org.elasticsearch.index.query.MatchAllQueryParser;
import org.elasticsearch.index.query.MatchQueryParser;
import org.elasticsearch.index.query.MissingQueryParser;
import org.elasticsearch.index.query.MoreLikeThisQueryParser;
import org.elasticsearch.index.query.MultiMatchQueryParser;
import org.elasticsearch.index.query.NotQueryParser;
import org.elasticsearch.index.query.OrQueryParser;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.PrefixQueryParser;
import org.elasticsearch.index.query.QueryFilterParser;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryStringQueryParser;
import org.elasticsearch.index.query.RangeQueryParser;
import org.elasticsearch.index.query.RegexpQueryParser;
import org.elasticsearch.index.query.ScriptQueryParser;
import org.elasticsearch.index.query.SimpleQueryStringParser;
import org.elasticsearch.index.query.SpanContainingQueryParser;
import org.elasticsearch.index.query.SpanFirstQueryParser;
import org.elasticsearch.index.query.SpanMultiTermQueryParser;
import org.elasticsearch.index.query.SpanNearQueryParser;
import org.elasticsearch.index.query.SpanNotQueryParser;
import org.elasticsearch.index.query.SpanOrQueryParser;
import org.elasticsearch.index.query.SpanTermQueryParser;
import org.elasticsearch.index.query.SpanWithinQueryParser;
import org.elasticsearch.index.query.TermQueryParser;
import org.elasticsearch.index.query.TermsQueryParser;
import org.elasticsearch.index.query.TypeQueryParser;
import org.elasticsearch.index.query.WildcardQueryParser;
import org.elasticsearch.index.query.WrapperQueryParser;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;

public class TestContext {
	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Index index = new Index("test");
		Settings settings = Settings.builder()
				.put("index.version.created", Version.V_1_7_6)
				.put("index.percolator.map_unmapped_fields_as_string", true)
				.build();
		IndexSettingsService indexSettingsService = new IndexSettingsService(index, settings);
		
		RAMDirectory rDir = new RAMDirectory();
		MyIndicesModule indicesModule = new MyIndicesModule();
		try {
			
			//设置默认分词器
			IndicesAnalysisService indicesAnalysisService = new IndicesAnalysisService(settings);
			Map<String, AnalyzerProviderFactory> analyzerProviderFactoryMap = new HashMap<>(1);
			analyzerProviderFactoryMap.put("default", new PreBuiltAnalyzerProviderFactory("default", AnalyzerScope.INDICES, new WhitespaceAnalyzer()));
			AnalysisService analysisService = new AnalysisService(index, indexSettingsService,indicesAnalysisService,analyzerProviderFactoryMap,null,null, null);
			
			//解析mapping
			InputStream is = TestContext.class.getClassLoader().getResourceAsStream("mapping.json");
			byte[] mappingBytes = new byte[is.available()];
			is.read(mappingBytes);
			
			MapperService mapperService = new MapperService(index,
		            settings,
		            analysisService,
		            null,
		            null,
		            indicesModule.getMapperRegistry());
			mapperService.close();
			
			DocumentMapperParser mapperParser = mapperService.documentMapperParser();
			InputStream is1 = TestContext.class.getClassLoader().getResourceAsStream("data.json");
			byte[] dataBytes = new byte[is1.available()];
			is1.read(dataBytes);
			DocumentMapper mapper = mapperParser.parse("test_type", new CompressedXContent(new BytesArray(mappingBytes)));
			Mapping mapping = mapper.mapping();
			mapperService.merge("test_type", new CompressedXContent(mapping.toBytes()), MergeReason.MAPPING_UPDATE, false);
			ParsedDocument docs = mapper.parse("test", "test_type", "1", new BytesArray(dataBytes));
			
			//生成document
			IndexWriter writer = new IndexWriter(rDir, new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(OpenMode.CREATE));
			writer.addDocuments(docs.docs());
			writer.close();
			
			//解析查询解析器
			Set<Class<? extends QueryParser>> queryParserClasses = indicesModule.getQueryParser();
			Set<QueryParser> queryParsers= new HashSet<>(64);
			for(Class<? extends QueryParser> clz : queryParserClasses){
				Constructor<?>[] constructors = clz.getConstructors();
				if(constructors.length > 0){
					Constructor<?> constructor = constructors[0];
					Class<?>[] typeClasses = constructor.getParameterTypes();
					if(typeClasses.length == 0){
						queryParsers.add(clz.newInstance());
					}else if(typeClasses[0].equals(Settings.class)){
						queryParsers.add((QueryParser)constructor.newInstance(settings));
					}
				}else{
					queryParsers.add(clz.newInstance());
				}
			}
			
			//查询
			IndicesQueriesRegistry indicesQueriesRegistry = new IndicesQueriesRegistry(settings, queryParsers);
			IndexFieldDataService indexFieldDataService = new IndexFieldDataService(index, indexSettingsService, null, new NoneCircuitBreakerService(), mapperService);
			IndexQueryParserService indexQueryParserService = new IndexQueryParserService(index, indexSettingsService, indicesQueriesRegistry, null, analysisService, mapperService, null, indexFieldDataService, null, null);
			DirectoryReader dreader = DirectoryReader.open(rDir);
			IndexSearcher searcher = new IndexSearcher(dreader);
			InputStream is2 = TestContext.class.getClassLoader().getResourceAsStream("search-type.json");
			byte[] searchBytes = new byte[is2.available()];
			is2.read(searchBytes);
			ParsedQuery parsedQuery = indexQueryParserService.parse(new BytesArray(searchBytes));
			Query query = parsedQuery.query();
			TopDocs tDocs = searcher.search(query, 5);
			System.out.println(tDocs.totalHits);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	static class MyIndicesModule {

	    private final Set<Class<? extends QueryParser>> queryParsers = new HashSet<>(64);
	    private final Map<String, Mapper.TypeParser> mapperParsers = new LinkedHashMap<>();
	    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers = new LinkedHashMap<>();

	    public MyIndicesModule() {
	        registerBuiltinQueryParsers();
	        registerBuiltInMappers();
	        registerBuiltInMetadataMappers();
	    }

	    private void registerBuiltinQueryParsers() {
	        registerQueryParser(MatchQueryParser.class);
	        registerQueryParser(MultiMatchQueryParser.class);
//	        registerQueryParser(NestedQueryParser.class);
//	        registerQueryParser(HasChildQueryParser.class);
//	        registerQueryParser(HasParentQueryParser.class);
	        registerQueryParser(DisMaxQueryParser.class);
	        registerQueryParser(IdsQueryParser.class);
	        registerQueryParser(MatchAllQueryParser.class);
	        registerQueryParser(QueryStringQueryParser.class);
	        registerQueryParser(BoostingQueryParser.class);
	        registerQueryParser(BoolQueryParser.class);
	        registerQueryParser(TermQueryParser.class);
	        registerQueryParser(TermsQueryParser.class);
	        registerQueryParser(FuzzyQueryParser.class);
	        registerQueryParser(RegexpQueryParser.class);
	        registerQueryParser(RangeQueryParser.class);
	        registerQueryParser(PrefixQueryParser.class);
	        registerQueryParser(WildcardQueryParser.class);
	        registerQueryParser(FilteredQueryParser.class);
	        registerQueryParser(ConstantScoreQueryParser.class);
	        registerQueryParser(SpanTermQueryParser.class);
	        registerQueryParser(SpanNotQueryParser.class);
	        registerQueryParser(SpanWithinQueryParser.class);
	        registerQueryParser(SpanContainingQueryParser.class);
	        registerQueryParser(FieldMaskingSpanQueryParser.class);
	        registerQueryParser(SpanFirstQueryParser.class);
	        registerQueryParser(SpanNearQueryParser.class);
	        registerQueryParser(SpanOrQueryParser.class);
	        registerQueryParser(MoreLikeThisQueryParser.class);
	        registerQueryParser(WrapperQueryParser.class);
//	        registerQueryParser(IndicesQueryParser.class);
	        registerQueryParser(CommonTermsQueryParser.class);
	        registerQueryParser(SpanMultiTermQueryParser.class);
//	        registerQueryParser(FunctionScoreQueryParser.class);
	        registerQueryParser(SimpleQueryStringParser.class);
//	        registerQueryParser(TemplateQueryParser.class);
	        registerQueryParser(TypeQueryParser.class);
	        registerQueryParser(LimitQueryParser.class);
	        registerQueryParser(ScriptQueryParser.class);
	        registerQueryParser(GeoDistanceQueryParser.class);
	        registerQueryParser(GeoDistanceRangeQueryParser.class);
	        registerQueryParser(GeoBoundingBoxQueryParser.class);
	        registerQueryParser(GeohashCellQuery.Parser.class);
	        registerQueryParser(GeoPolygonQueryParser.class);
	        registerQueryParser(QueryFilterParser.class);
	        registerQueryParser(FQueryFilterParser.class);
	        registerQueryParser(AndQueryParser.class);
	        registerQueryParser(OrQueryParser.class);
	        registerQueryParser(NotQueryParser.class);
	        registerQueryParser(ExistsQueryParser.class);
	        registerQueryParser(MissingQueryParser.class);

	        if (ShapesAvailability.JTS_AVAILABLE) {
	            registerQueryParser(GeoShapeQueryParser.class);
	        }
	    }

	    private void registerBuiltInMappers() {
	        registerMapper(ByteFieldMapper.CONTENT_TYPE, new ByteFieldMapper.TypeParser());
	        registerMapper(ShortFieldMapper.CONTENT_TYPE, new ShortFieldMapper.TypeParser());
	        registerMapper(IntegerFieldMapper.CONTENT_TYPE, new IntegerFieldMapper.TypeParser());
	        registerMapper(LongFieldMapper.CONTENT_TYPE, new LongFieldMapper.TypeParser());
	        registerMapper(FloatFieldMapper.CONTENT_TYPE, new FloatFieldMapper.TypeParser());
	        registerMapper(DoubleFieldMapper.CONTENT_TYPE, new DoubleFieldMapper.TypeParser());
	        registerMapper(BooleanFieldMapper.CONTENT_TYPE, new BooleanFieldMapper.TypeParser());
	        registerMapper(BinaryFieldMapper.CONTENT_TYPE, new BinaryFieldMapper.TypeParser());
	        registerMapper(DateFieldMapper.CONTENT_TYPE, new DateFieldMapper.TypeParser());
	        registerMapper(IpFieldMapper.CONTENT_TYPE, new IpFieldMapper.TypeParser());
	        registerMapper(StringFieldMapper.CONTENT_TYPE, new StringFieldMapper.TypeParser());
	        registerMapper(TokenCountFieldMapper.CONTENT_TYPE, new TokenCountFieldMapper.TypeParser());
	        registerMapper(ObjectMapper.CONTENT_TYPE, new ObjectMapper.TypeParser());
	        registerMapper(ObjectMapper.NESTED_CONTENT_TYPE, new ObjectMapper.TypeParser());
	        registerMapper(TypeParsers.MULTI_FIELD_CONTENT_TYPE, TypeParsers.multiFieldConverterTypeParser);
	        registerMapper(CompletionFieldMapper.CONTENT_TYPE, new CompletionFieldMapper.TypeParser());
	        registerMapper(GeoPointFieldMapper.CONTENT_TYPE, new GeoPointFieldMapper.TypeParser());

	        if (ShapesAvailability.JTS_AVAILABLE) {
	            registerMapper(GeoShapeFieldMapper.CONTENT_TYPE, new GeoShapeFieldMapper.TypeParser());
	        }
	    }

	    private void registerBuiltInMetadataMappers() {
	        // NOTE: the order is important

	        // UID first so it will be the first stored field to load (so will benefit from "fields: []" early termination
	        registerMetadataMapper(UidFieldMapper.NAME, new UidFieldMapper.TypeParser());
	        registerMetadataMapper(IdFieldMapper.NAME, new IdFieldMapper.TypeParser());
	        registerMetadataMapper(RoutingFieldMapper.NAME, new RoutingFieldMapper.TypeParser());
	        registerMetadataMapper(IndexFieldMapper.NAME, new IndexFieldMapper.TypeParser());
	        registerMetadataMapper(SourceFieldMapper.NAME, new SourceFieldMapper.TypeParser());
	        registerMetadataMapper(TypeFieldMapper.NAME, new TypeFieldMapper.TypeParser());
	        registerMetadataMapper(AllFieldMapper.NAME, new AllFieldMapper.TypeParser());
	        registerMetadataMapper(TimestampFieldMapper.NAME, new TimestampFieldMapper.TypeParser());
	        registerMetadataMapper(TTLFieldMapper.NAME, new TTLFieldMapper.TypeParser());
	        registerMetadataMapper(VersionFieldMapper.NAME, new VersionFieldMapper.TypeParser());
	        registerMetadataMapper(ParentFieldMapper.NAME, new ParentFieldMapper.TypeParser());
	        // _field_names is not registered here, see #getMapperRegistry: we need to register it
	        // last so that it can see all other mappers, including those coming from plugins
	    }

	    public void registerQueryParser(Class<? extends QueryParser> queryParser) {
	    	queryParsers.add(queryParser);
	    }

	    public synchronized void registerMapper(String type, Mapper.TypeParser parser) {
	        if (mapperParsers.containsKey(type)) {
	            throw new IllegalArgumentException("A mapper is already registered for type [" + type + "]");
	        }
	        mapperParsers.put(type, parser);
	    }

	    public synchronized void registerMetadataMapper(String name, MetadataFieldMapper.TypeParser parser) {
	        if (metadataMapperParsers.containsKey(name)) {
	            throw new IllegalArgumentException("A mapper is already registered for metadata mapper [" + name + "]");
	        }
	        metadataMapperParsers.put(name, parser);
	    }

	    public synchronized MapperRegistry getMapperRegistry() {
	        if (metadataMapperParsers.containsKey(FieldNamesFieldMapper.NAME)) {
	            throw new IllegalStateException("Metadata mapper [" + FieldNamesFieldMapper.NAME + "] is already registered");
	        }
	        final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers
	            = new LinkedHashMap<>(this.metadataMapperParsers);
	        metadataMapperParsers.put(FieldNamesFieldMapper.NAME, new FieldNamesFieldMapper.TypeParser());
	        return new MapperRegistry(mapperParsers, metadataMapperParsers);
	    }
	    
	    public synchronized Set<Class<? extends QueryParser>> getQueryParser(){
	    	return this.queryParsers;
	    }
	}
}
