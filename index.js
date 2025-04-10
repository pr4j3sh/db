#!/usr/bin/env node

const { greet } = require("./src/utils");

console.log(greet("frames"));

module.exports = {
  greet,
};
