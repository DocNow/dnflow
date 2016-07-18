var path = require('path')
var webpack = require('webpack')

module.exports = {
  entry: {
    app: [
      // './static/js/index.js',
      './static/js/flux-sketch-1.js'
    ],
  },
  debug: true,
  output: {
    path: path.join(__dirname, 'static/js'),
    filename: 'bundle-flux-sketch.js',
  },
  module: {
    loaders: [
      {
        test: /\.jsx?$/,
        loaders: ['babel-loader'],
        include: path.join(__dirname, 'static/js'),
        exclude: /node_modules/
      }
    ]
  },
  resolve: {
    extensions: ['', '.js']
  },
  devtool: '#inline-source-map'
};
