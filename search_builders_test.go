package redis_test

import (
	"context"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("RediSearch Builders", Label("search", "builders"), func() {
	ctx := context.Background()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{Addr: ":6379", Protocol: 2})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		expectCloseErr := client.Close()
		Expect(expectCloseErr).NotTo(HaveOccurred())
	})

	It("should create index and search with scores using builders", Label("search", "ftcreate", "ftsearch"), func() {
		createVal, err := client.CreateIndex(ctx, "idx1").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "foo", FieldType: redis.SearchFieldTypeText}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(createVal).To(Equal("OK"))

		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "foo", "bar")
		client.HSet(ctx, "doc2", "foo", "baz")

		res, err := client.Search(ctx, "idx1", "foo").WithScores().Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(Equal(int64(2)))
		for _, doc := range res.Docs {
			Expect(*doc.Score).To(BeNumerically(">", 0))
		}
	})

	It("should aggregate using builders", Label("search", "ftaggregate"), func() {
		_, err := client.CreateIndex(ctx, "idx2").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "n", FieldType: redis.SearchFieldTypeNumeric}).
			Run()
		Expect(err).NotTo(HaveOccurred())
		WaitForIndexing(client, "idx2")

		client.HSet(ctx, "d1", "n", 1)
		client.HSet(ctx, "d2", "n", 2)

		agg, err := client.Aggregate(ctx, "idx2", "*").
			GroupBy("@n").
			ReduceAs(redis.SearchCount, "", 0).
			Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(agg.Rows)).To(Equal(2))
	})

	It("should drop index using builder", Label("search", "ftdropindex"), func() {
		Expect(client.CreateIndex(ctx, "idx3").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "x", FieldType: redis.SearchFieldTypeText}).
			Run()).To(Equal("OK"))
		WaitForIndexing(client, "idx3")

		dropVal, err := client.DropIndex(ctx, "idx3").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(dropVal).To(Equal("OK"))
	})

	It("should manage aliases using builder", Label("search", "ftalias"), func() {
		Expect(client.CreateIndex(ctx, "idx4").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "t", FieldType: redis.SearchFieldTypeText}).
			Run()).To(Equal("OK"))
		WaitForIndexing(client, "idx4")

		addVal, err := client.AliasAdd(ctx, "alias1", "idx4").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(addVal).To(Equal("OK"))

		_, err = client.Search(ctx, "alias1", "*").Run()
		Expect(err).NotTo(HaveOccurred())

		delVal, err := client.AliasDel(ctx, "alias1").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(delVal).To(Equal("OK"))
	})

	It("should explain query using ExplainBuilder", Label("search", "ftexplain"), func() {
		expl, err := client.Explain(ctx, "idx1", "foo").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(expl).To(ContainSubstring("INTERPRET"))
	})

	It("should retrieve info using SearchInfo builder", Label("search", "ftinfo"), func() {
		i, err := client.SearchInfo(ctx, "idx1").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(i.IndexName).To(Equal("idx1"))
	})

	It("should spellcheck using builder", Label("search", "ftspellcheck"), func() {
		_, err := client.SpellCheck(ctx, "idx1", "ba").Distance(1).Run()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should manage dictionary using DictBuilder", Label("search", "ftdict"), func() {
		addCount, err := client.DictAdd(ctx, "dict1", "a", "b").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(addCount).To(Equal(int64(2)))

		dump, err := client.DictDump(ctx, "dict1").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(dump).To(ContainElements("a", "b"))

		delCount, err := client.DictDel(ctx, "dict1", "a").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(delCount).To(Equal(int64(1)))
	})

	It("should tag values using TagValsBuilder", Label("search", "fttagvals"), func() {
		vals, err := client.TagVals(ctx, "idx1", "foo").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(vals).To(BeAssignableToTypeOf([]string{}))
	})

	It("should cursor read and delete using CursorBuilder", Label("search", "ftcursor"), func() {
		Expect(client.CreateIndex(ctx, "idx5").
			OnHash().
			Schema(&redis.FieldSchema{FieldName: "f", FieldType: redis.SearchFieldTypeText}).
			Run()).To(Equal("OK"))
		WaitForIndexing(client, "idx5")
		client.HSet(ctx, "d", "f", "x")

		_, err := client.CursorRead(ctx, "idx5", 0).Run()
		Expect(err).NotTo(HaveOccurred())

		_, err = client.CursorDel(ctx, "idx5", 0).Run()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should update synonyms using SynUpdateBuilder", Label("search", "ftsynupdate"), func() {
		syn, err := client.SynUpdate(ctx, "idx1", "grp1").Terms("a", "b").Run()
		Expect(err).NotTo(HaveOccurred())
		Expect(syn).To(Equal("OK"))
	})
})
