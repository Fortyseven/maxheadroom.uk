const markdownIt = require("markdown-it");
const markdownItAttrs = require("markdown-it-attrs");

const markdownItOptions = {
    html: true,
    // breaks: true,
    linkify: true,
};

module.exports = function (eleventyConfig) {
    eleventyConfig.addPassthroughCopy("./src/assets");
    eleventyConfig.addPassthroughCopy("./src/css/");

    eleventyConfig.addWatchTarget("./src/assets/");
    eleventyConfig.addWatchTarget("./src/css/");

    eleventyConfig.addShortcode("year", () => `${new Date().getFullYear()}`);

    const markdownLib = markdownIt(markdownItOptions)
        .use(markdownItAttrs)
        .disable("code");
    eleventyConfig.setLibrary("md", markdownLib);

    return {
        dir: {
            input: "src",
            output: "public",
        },
        templateFormats: ["html", "md", "njk"],
    };
};
