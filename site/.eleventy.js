module.exports = function (eleventyConfig) {
    eleventyConfig.addPassthroughCopy("./src/assets");
    eleventyConfig.addPassthroughCopy("./src/css/");

    eleventyConfig.addWatchTarget("./src/assets/");
    eleventyConfig.addWatchTarget("./src/css/");

    eleventyConfig.addShortcode("year", () => `${new Date().getFullYear()}`);

    return {
        dir: {
            input: "src",
            output: "public",
        },
    };
};
