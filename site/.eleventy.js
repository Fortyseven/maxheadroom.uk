const markdownIt = require("markdown-it");
const markdownItAttrs = require("markdown-it-attrs");
const markdownItFigures = require("markdown-it-image-figures");
const pageAssetsPlugin = require("eleventy-plugin-page-assets");

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

    // Co-located images (next to a content file) are copied to that page's
    // permalink output dir, so relative refs like ./image.png resolve correctly.
    // Passthrough copy can't do this — it mirrors the source tree, ignoring permalink.
    eleventyConfig.addPlugin(pageAssetsPlugin, {
        mode: "parse",
        postsMatching: "src/**/*.md",
        assetsMatching: "*.png|*.jpg|*.jpeg|*.gif|*.webp",
        recursive: false,
        hashAssets: false,
    });

    eleventyConfig.addShortcode("year", () => `${new Date().getFullYear()}`);

    const markdownLib = markdownIt(markdownItOptions)
        .use(markdownItAttrs)
        // Wrap standalone images in <figure> and render alt text as <figcaption>.
        .use(markdownItFigures, { figcaption: "alt", copyAttrs: "^class$" })
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
