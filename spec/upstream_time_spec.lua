-- Parser for compound NGINX upstream timing values (design.md §10).
-- Calling tonumber() on "0.005, 0.010" silently produces nil or garbage;
-- this parser is explicit about every token.

local parser = require "resty.adaptive_limit.upstream_time"

describe("upstream_time.parse", function()
    it("returns nil for empty and absent values", function()
        assert.Nil(parser.parse(nil, "last"))
        assert.Nil(parser.parse("", "last"))
        assert.Nil(parser.parse("-", "last"))
        assert.Nil(parser.parse("   ", "last"))
    end)

    it("parses a single value for every choice", function()
        assert.are.equal(0.005, parser.parse("0.005", "last"))
        assert.are.equal(0.005, parser.parse("0.005", "max"))
        assert.are.equal(0.005, parser.parse("0.005", "sum"))
    end)

    it("parses the documented retry format 'a, b'", function()
        assert.are.equal(0.010, parser.parse("0.005, 0.010", "last"))
        assert.are.equal(0.010, parser.parse("0.005, 0.010", "max"))
        assert.are.equal(0.015, parser.parse("0.005, 0.010", "sum"))
    end)

    it("parses comma-space and space-only separators alike", function()
        -- $upstream_response_time separates attempts with ", "
        assert.are.equal(0.02, parser.parse("0.005 , 0.020", "last"))
        -- some variables join with plain spaces on older builds
        assert.are.equal(0.02, parser.parse("0.005 0.020", "last"))
    end)

    it("handles three attempts", function()
        assert.are.equal(0.030, parser.parse("0.010, 0.020, 0.030", "last"))
        assert.are.equal(0.030, parser.parse("0.010, 0.020, 0.030", "max"))
        assert.are.equal(0.060, parser.parse("0.010, 0.020, 0.030", "sum"))
    end)

    it("rejects values containing '-' attempts instead of guessing", function()
        assert.Nil(parser.parse("0.005, -", "last"))
    end)

    it("rejects garbage tokens entirely (never a wrong number)", function()
        assert.Nil(parser.parse("0.005, 12ms", "last"))
        assert.Nil(parser.parse("abc", "last"))
        assert.Nil(parser.parse("0.005, NaN", "last"))
    end)

    it("accepts zero durations", function()
        assert.are.equal(0, parser.parse("0.000", "last"))
        assert.are.equal(0, parser.parse("0, 0", "max"))
    end)
end)
